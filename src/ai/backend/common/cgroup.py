import asyncio
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, Optional

import aiohttp

from .docker import get_docker_connector
from .logging import BraceStyleAdapter
from .types import PID, ContainerId

log = BraceStyleAdapter(logging.getLogger(__spec__.name)) 


@dataclass
class CgroupVersion:
    version: Literal["v1"] | Literal["v2"]
    driver: Literal["systemd"] | Literal["cgroupfs"]


async def get_docker_cgroup_version() -> CgroupVersion:
    connector = get_docker_connector()
    async with aiohttp.ClientSession(connector=connector.connector) as sess:
        async with sess.get(connector.docker_host / "info") as resp:
            data = await resp.json()
            return CgroupVersion(data["CgroupVersion"], data["CgroupDriver"])


def get_cgroup_mount_point(version: str, controller: str) -> Path:
    for line in Path("/proc/mounts").read_text().splitlines():
        device, mount_point, fstype, options, _ = line.split(" ", 4)
        match version:
            case "1":
                if fstype == "cgroup":
                    if controller in options.split(","):
                        return Path(mount_point)
            case "2":
                if fstype == "cgroup2":
                    return Path(mount_point)
    raise RuntimeError("could not find the cgroup mount point")


def get_cgroup_controller_id(controller: str) -> str:
    for line in Path("/proc/cgroups").read_text().splitlines():
        name, id, _ = line.split("\t", 2)
        if name == controller:
            return id
    raise RuntimeError(f"could not find the cgroup controller {controller}")


def get_cgroup_of_pid(controller: str, pid: PID) -> str:
    controller_id = get_cgroup_controller_id(controller)
    for line in Path(f"/proc/{pid}/cgroup").read_text().splitlines():
        id, name, cgroup = line.split(":", 2)
        if id == controller_id:
            return cgroup.removeprefix("/")
    raise RuntimeError(f"could not find the cgroup of PID {pid}")