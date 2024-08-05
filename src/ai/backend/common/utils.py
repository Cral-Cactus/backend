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


def str_to_timedelta(tstr: str) -> timedelta:
    _rx = re.compile(
        r"(?P<sign>[+|-])?\s*"
        r"((?P<days>\d+(\.\d+)?)(d|day|days))?\s*"
        r"((?P<hours>\d+(\.\d+)?)(h|hr|hrs|hour|hours))?\s*"
        r"((?P<minutes>\d+(\.\d+)?)(m|min|mins|minute|minutes))?\s*"
        r"((?P<seconds>\d+(\.\d+)?)(s|sec|secs|second|seconds))?$"
    )
    match = _rx.match(tstr)
    if not match:
        try:
            return timedelta(seconds=float(tstr))
        except TypeError:
            pass
        raise ValueError("Invalid time expression")
    groups = match.groupdict()
    sign = groups.pop("sign", None)
    if set(groups.values()) == {None}:
        raise ValueError("Invalid time expression")
    params = {n: -float(t) if sign == "-" else float(t) for n, t in groups.items() if t}
    return timedelta(**params)