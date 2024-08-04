import json
import logging
import os
import socket
import sys
from ipaddress import _BaseAddress as BaseIPAddress
from ipaddress import _BaseNetwork as BaseIPNetwork
from ipaddress import ip_address
from pathlib import Path, PosixPath
from typing import Awaitable, Callable, Iterable, Optional

import aiodns
import ifaddr
import psutil

from .utils import curl

__all__ = (
    "detect_cloud",
    "current_provider",
    "get_instance_id",
    "get_instance_ip",
    "get_instance_type",
    "get_instance_region",
    "get_root_fs_type",
    "get_wsl_version",
)

log = logging.getLogger(__spec__.name)


def is_containerized() -> bool:
    try:
        cginfo = Path("/proc/self/cgroup").read_text()
        if "/docker/" in cginfo or "/lxc/" in cginfo:
            return True
        return False
    except IOError:
        return False


def detect_cloud() -> Optional[str]:
    if sys.platform.startswith("linux"):
        try:
            mb = Path("/sys/devices/virtual/dmi/id/board_vendor").read_text().lower()
            if "amazon" in mb:
                return "amazon"
        except IOError:
            pass
        try:
            bios = Path("/sys/devices/virtual/dmi/id/bios_version").read_text().lower()
            if "google" in bios:
                return "google"
            if "amazon" in bios:
                return "amazon"
        except IOError:
            pass
        try:
            dhcp = Path("/var/lib/dhcp/dhclient.eth0.leases").read_text()
            if "unknown-245" in dhcp:
                return "azure"
        except IOError:
            pass
    return None


def fetch_local_ipaddrs(cidr: BaseIPNetwork) -> Iterable[BaseIPAddress]:
    proto = socket.AF_INET if cidr.version == 4 else socket.AF_INET6
    for adapter in ifaddr.get_adapters():
        if not adapter.ips:
            continue
        for entry in adapter.ips:
            if entry.is_IPv4 and proto == socket.AF_INET:
                assert isinstance(entry.ip, str)
                addr = ip_address(entry.ip)
            elif entry.is_IPv6 and proto == socket.AF_INET6:
                assert isinstance(entry.ip, tuple)
                addr = ip_address(entry.ip[0])
            else:
                continue
            if addr in cidr:
                yield addr


def get_root_fs_type() -> tuple[PosixPath, str]:
    for partition in psutil.disk_partitions():
        if partition.mountpoint == "/":
            return PosixPath(partition.device), partition.fstype
    raise RuntimeError("Could not find the root filesystem from the mounts.")


def get_wsl_version() -> int:
    if not Path("/proc/sys/fs/binfmt_misc/WSLInterop").exists() and not Path("/run/WSL").exists():
        return 0
    try:
        _, root_fs_type = get_root_fs_type()
    except RuntimeError:
        return 2
    if root_fs_type in ("wslfs", "lxfs"):
        return 1
    return 2

current_provider = detect_cloud()
if current_provider is None:
    log.info("Detected environment: on-premise setup")
    log.info("The agent node ID is set using the hostname.")
else:
    log.info(f"Detected environment: {current_provider} cloud")
    log.info("The agent node ID will follow the instance ID.")