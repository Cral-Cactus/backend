from __future__ import annotations

import asyncio
import base64
import numbers
import random
import re
import sys
import uuid
from collections import OrderedDict
from datetime import timedelta
from itertools import chain
from pathlib import Path
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncIterator,
    Iterable,
    Iterator,
    Mapping,
    Tuple,
    TypeVar,
    Union,
)

import aiofiles
from async_timeout import timeout

if TYPE_CHECKING:
    from decimal import Decimal

    from aiofiles.threadpool.text import AsyncTextIOWrapper

from .asyncio import (
    AsyncBarrier,
    cancel_tasks,
    current_loop,
    run_through,
)
from .enum_extension import StringSetFlag
from .files import AsyncFileWriter 
from .networking import curl, find_free_port
from .types import BinarySize
from .exception import VolumeMountFailed, VolumeUnmountFailed
from .defs import DEFAULT_FILE_IO_TIMEOUT

KT = TypeVar("KT")
VT = TypeVar("VT")


def env_info() -> str:
    v = sys.version_info
    pyver = f"Python {v.major}.{v.minor}.{v.micro}"
    if v.releaselevel == "alpha":
        pyver += "a"
    if v.releaselevel == "beta":
        pyver += "b"
    if v.releaselevel == "candidate":
        pyver += "rc"
    if v.releaselevel != "final":
        pyver += str(v.serial)
    return f"{pyver} (env: {sys.prefix})"


def odict(*args: Tuple[KT, VT]) -> OrderedDict[KT, VT]:
    return OrderedDict(args)


def dict2kvlist(o: Mapping[KT, VT]) -> Iterable[Union[KT, VT]]:
    return chain.from_iterable((k, v) for k, v in o.items())


def generate_uuid() -> str:
    u = uuid.uuid4()
    return base64.urlsafe_b64encode(u.bytes)[:-2].decode("ascii")


def get_random_seq(length: float, num_points: int, min_distance: float) -> Iterator[float]:
    assert num_points * min_distance <= length + min_distance, (
        "There are too many points or it has a too large distance which cannot be fit into the"
        " given length."
    )
    extra = length - (num_points - 1) * min_distance
    ro = [random.uniform(0, 1) for _ in range(num_points + 1)]
    sum_ro = sum(ro)
    rn = [extra * r / sum_ro for r in ro[0:num_points]]
    spacing = [min_distance + rn[i] for i in range(num_points)]
    cumulative_sum = 0.0
    for s in spacing:
        cumulative_sum += s
        yield cumulative_sum - min_distance


def nmget(
    o: Mapping[str, Any],
    key_path: str,
    def_val: Any = None,
    path_delimiter: str = ".",
    null_as_default: bool = True,
) -> Any:
    pieces = key_path.split(path_delimiter)
    while pieces:
        p = pieces.pop(0)
        if o is None or p not in o:
            return def_val
        o = o[p]
    if o is None and null_as_default:
        return def_val
    return o


def readable_size_to_bytes(expr: Any) -> BinarySize | Decimal:
    if isinstance(expr, numbers.Integral):
        return BinarySize(expr)
    return BinarySize.from_str(expr)


class FstabEntry:
    def __init__(
        self, device: str, mountpoint: str, fstype: str, options: str | None, d=0, p=0
    ) -> None:
        self.device = device
        self.mountpoint = mountpoint
        self.fstype = fstype
        if not options:
            options = "defaults"
        self.options = options
        self.d = d
        self.p = p

    def __eq__(self, o):
        return str(self) == str(o)

    def __str__(self):
        return "{} {} {} {} {} {}".format(
            self.device, self.mountpoint, self.fstype, self.options, self.d, self.p
        )


class Fstab:
    def __init__(self, fp: AsyncTextIOWrapper) -> None:
        self._fp = fp

    def _hydrate_entry(self, line: str) -> FstabEntry:
        return FstabEntry(*[x for x in line.strip("\n").split(" ") if x not in ("", None)])

    async def get_entries(self) -> AsyncIterator[FstabEntry]:
        await self._fp.seek(0)
        for line in await self._fp.readlines():
            try:
                line = line.strip()
                if not line.startswith("#"):
                    yield self._hydrate_entry(line)
            except TypeError:
                pass

    async def get_entry_by_attr(self, attr: str, value: Any) -> FstabEntry | None:
        async for entry in self.get_entries():
            e_attr = getattr(entry, attr)
            if e_attr == value:
                return entry
        return None

    async def add_entry(self, entry: FstabEntry) -> None:
        if await self.get_entry_by_attr("device", entry.device):
            return
        await self._fp.write(str(entry) + "\n")
        await self._fp.truncate()

    async def add(
        self, device: str, mountpoint: str, fstype: str, options: str | None = None, d=0, p=0
    ) -> None:
        return await self.add_entry(FstabEntry(device, mountpoint, fstype, options, d, p))

    async def remove_entry(self, entry: FstabEntry) -> bool:
        await self._fp.seek(0)
        lines = await self._fp.readlines()
        line_no: int | None = None
        for index, line in enumerate(lines):
            if not line.strip().startswith("#"):
                try:
                    if self._hydrate_entry(line) == entry:
                        line_no = index
                        break
                except TypeError:
                    pass
        else:
            return False
        assert line_no is not None
        del lines[line_no]
        await self._fp.seek(0)
        await self._fp.write("".join(lines))
        await self._fp.truncate()
        return True

    async def remove_by_mountpoint(self, mountpoint: str) -> bool:
        entry = await self.get_entry_by_attr("mountpoint", mountpoint)
        if entry:
            return await self.remove_entry(entry)
        return False


async def mount(
    mount_path: str,
    fs_location: str,
    fs_type: str = "nfs",
    cmd_options: str | None = None,
    edit_fstab: bool = False,
    fstab_path: str | None = None,
    mount_prefix: str | None = None,
) -> None:
    if mount_prefix is None:
        mount_prefix = "/"
    if fstab_path is None:
        fstab_path = "/etc/fstab"
    mountpoint = Path(mount_prefix) / mount_path
    mountpoint.mkdir(exist_ok=True)
    if cmd_options is not None:
        cmd = [
            "mount",
            "-t",
            fs_type,
            "-o",
            cmd_options,
            fs_location,
            str(mountpoint),
        ]
    else:
        cmd = ["mount", "-t", fs_type, fs_location, str(mountpoint)]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE
    )
    raw_out, raw_err = await proc.communicate()
    raw_out.decode("utf8")
    err = raw_err.decode("utf8")
    await proc.wait()
    if err:
        raise VolumeMountFailed(f"Failed to mount {fs_location} on {mountpoint}. (e: {err})")
    if edit_fstab:
        async with aiofiles.open(fstab_path, mode="r+") as fp:
            fstab = Fstab(fp)
            await fstab.add(
                fs_location,
                str(mountpoint),
                fs_type,
                cmd_options,
            )


async def umount(
    mount_path: str,
    mount_prefix: str | None = None,
    edit_fstab: bool = False,
    fstab_path: str | None = None,
    rmdir_if_empty: bool = False,
    *,
    timeout_sec: float | None = DEFAULT_FILE_IO_TIMEOUT,
) -> bool:
    if mount_prefix is None:
        mount_prefix = "/"
    if fstab_path is None:
        fstab_path = "/etc/fstab"
    mountpoint = Path(mount_prefix) / mount_path
    assert Path(mount_prefix) != mountpoint
    if not mountpoint.is_mount():
        return False
    try:
        with timeout(timeout_sec):
            proc = await asyncio.create_subprocess_exec(
                *[
                    "umount",
                    str(mountpoint),
                ],
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            raw_out, raw_err = await proc.communicate()
            raw_out.decode("utf8")
            err = raw_err.decode("utf8")
            await proc.wait()
    except asyncio.TimeoutError:
        raise VolumeUnmountFailed(
            f"Failed to umount {mountpoint}. Raise timeout ({timeout_sec}sec). "
        )
    if err:
        raise VolumeUnmountFailed(f"Failed to umount {mountpoint}")
    if rmdir_if_empty:
        try:
            mountpoint.rmdir()
        except OSError:
            pass
    if edit_fstab:
        async with aiofiles.open(fstab_path, mode="r+") as fp:
            fstab = Fstab(fp)
            await fstab.remove_by_mountpoint(str(mountpoint))
    return True