from __future__ import annotations

import asyncio
import json
import logging
import os
import pickle
import re
import shutil
import signal
import sys
import textwrap
import time
import traceback
import weakref
import zlib
from abc import ABCMeta, abstractmethod
from collections import defaultdict
from decimal import Decimal
from io import SEEK_END, BytesIO
from pathlib import Path
from uuid import UUID

import aiotools
import attrs
import pkg_resources
import yaml
import zmq
import zmq.asyncio
from async_timeout import timeout
from cachetools import LRUCache, cached
from redis.asyncio import Redis
from ai.backend.common.events_experimental import EventDispatcher as ExperimentalEventDispatcher
from ai.backend.common.exception import VolumeMountFailed
from ai.backend.common.lock import FileLock
from ai.backend.common.logging import BraceStyleAdapter, pretty
from ai.backend.common.plugin.monitor import ErrorPluginContext, StatsPluginContext
from ai.backend.common.service_ports import parse_service_ports

if TYPE_CHECKING:
    from ai.backend.common.auth import PublicKey
    from ai.backend.common.etcd import AsyncEtcd

log = BraceStyleAdapter(logging.getLogger(__spec__.name))

_sentinel = Sentinel.TOKEN

ACTIVE_STATUS_SET = frozenset([
    ContainerStatus.RUNNING,
    ContainerStatus.RESTARTING,
    ContainerStatus.PAUSED,
])

DEAD_STATUS_SET = frozenset([
    ContainerStatus.EXITED,
    ContainerStatus.DEAD,
    ContainerStatus.REMOVING,
])

COMMIT_STATUS_EXPIRE: Final[int] = 13
EVENT_DISPATCHER_CONSUMER_GROUP: Final = "agent"

KernelObjectType = TypeVar("KernelObjectType", bound=AbstractKernel)


class AbstractKernelCreationContext(aobject, Generic[KernelObjectType]):
    kspec_version: int
    kernel_id: KernelId
    session_id: SessionId
    agent_id: AgentId
    event_producer: EventProducer
    kernel_config: KernelCreationConfig
    local_config: Mapping[str, Any]
    kernel_features: FrozenSet[str]
    image_ref: ImageRef
    internal_data: Mapping[str, Any]
    restarting: bool
    cancellation_handlers: Sequence[Callable[[], Awaitable[None]]] = []
    _rx_distro = re.compile(r"\.([a-z-]+\d+\.\d+)\.")

    def __init__(
        self,
        kernel_id: KernelId,
        session_id: SessionId,
        agent_id: AgentId,
        event_producer: EventProducer,
        kernel_config: KernelCreationConfig,
        local_config: Mapping[str, Any],
        computers: MutableMapping[DeviceName, ComputerContext],
        restarting: bool = False,
    ) -> None:
        self.image_labels = kernel_config["image"]["labels"]
        self.kspec_version = int(self.image_labels.get("ai.backend.kernelspec", "1"))
        self.kernel_features = frozenset(self.image_labels.get("ai.backend.features", "").split())
        self.kernel_id = kernel_id
        self.session_id = session_id
        self.agent_id = agent_id
        self.event_producer = event_producer
        self.kernel_config = kernel_config
        self.image_ref = ImageRef(
            kernel_config["image"]["canonical"],
            known_registries=[kernel_config["image"]["registry"]["name"]],
            is_local=kernel_config["image"]["is_local"],
            architecture=kernel_config["image"].get("architecture", get_arch_name()),
        )
        self.internal_data = kernel_config["internal_data"] or {}
        self.computers = computers
        self.restarting = restarting
        self.local_config = local_config

    @abstractmethod
    async def get_extra_envs(self) -> Mapping[str, str]:
        return {}

    @abstractmethod
    async def prepare_resource_spec(
        self,
    ) -> Tuple[KernelResourceSpec, Optional[Mapping[str, Any]]]:
        raise NotImplementedError

    @abstractmethod
    async def prepare_scratch(self) -> None:
        pass

    @abstractmethod
    async def get_intrinsic_mounts(self) -> Sequence[Mount]:
        return []

    def update_user_bootstrap_script(self, script: str) -> None:
        self.kernel_config["bootstrap_script"] = script

    @abstractmethod
    async def apply_network(self, cluster_info: ClusterInfo) -> None:
        raise NotImplementedError

    @abstractmethod
    async def prepare_ssh(self, cluster_info: ClusterInfo) -> None:
        raise NotImplementedError

    @abstractmethod
    async def process_mounts(self, mounts: Sequence[Mount]):
        raise NotImplementedError

    @abstractmethod
    async def apply_accelerator_allocation(
        self,
        computer: AbstractComputePlugin,
        device_alloc: Mapping[SlotName, Mapping[DeviceId, Decimal]],
    ) -> None:
        raise NotImplementedError

    @abstractmethod
    async def generate_accelerator_mounts(
        self,
        computer: AbstractComputePlugin,
        device_alloc: Mapping[SlotName, Mapping[DeviceId, Decimal]],
    ) -> List[MountInfo]:
        raise NotImplementedError

    @abstractmethod
    def resolve_krunner_filepath(self, filename) -> Path:

        raise NotImplementedError

    @abstractmethod
    def get_runner_mount(
        self,
        type: MountTypes,
        src: Union[str, Path],
        target: Union[str, Path],
        perm: Literal["ro", "rw"] = "ro",
        opts: Mapping[str, Any] = None,
    ):

        raise NotImplementedError

    @abstractmethod
    async def spawn(
        self,
        resource_spec: KernelResourceSpec,
        environ: Mapping[str, str],
        service_ports,
    ) -> KernelObjectType:
        raise NotImplementedError

    @abstractmethod
    async def start_container(
        self,
        kernel_obj: AbstractKernel,
        cmdargs: List[str],
        resource_opts,
        preopen_ports,
    ) -> Mapping[str, Any]:
        raise NotImplementedError

    @cached(
        cache=LRUCache(maxsize=32),
        key=lambda self: (
            self.image_ref,
            self.kernel_config["image"]["labels"].get("ai.backend.base-distro", "ubuntu16.04"),
        ),
    )
    def get_krunner_info(self) -> Tuple[str, str, str, str, str]:
        image_labels = self.kernel_config["image"]["labels"]
        distro = image_labels.get("ai.backend.base-distro", "ubuntu16.04")
        matched_distro, krunner_volume = match_distro_data(
            self.local_config["container"]["krunner-volumes"], distro
        )
        matched_libc_style = "glibc"
        if distro.startswith("alpine"):
            matched_libc_style = "musl"
        krunner_pyver = "3.6"  # fallback
        if m := re.search(r"^([a-z-]+)(\d+(\.\d+)*)?$", matched_distro):
            matched_distro_pkgname = m.group(1).replace("-", "_")
            try:
                krunner_pyver = (
                    Path(
                        pkg_resources.resource_filename(
                            f"ai.backend.krunner.{matched_distro_pkgname}",
                            f"krunner-python.{matched_distro}.txt",
                        )
                    )
                    .read_text()
                    .strip()
                )
            except FileNotFoundError:
                pass
        log.debug("selected krunner: {}", matched_distro)
        log.debug("selected libc style: {}", matched_libc_style)
        log.debug("krunner volume: {}", krunner_volume)
        log.debug("krunner python: {}", krunner_pyver)
        arch = get_arch_name()
        return arch, matched_distro, matched_libc_style, krunner_volume, krunner_pyver