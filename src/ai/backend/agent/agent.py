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
        self.kspec_version = int(self.image_labels.get("backend.kernelspec", "1"))
        self.kernel_features = frozenset(self.image_labels.get("backend.features", "").split())
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
    async def mount_vfolders(
        self,
        vfolders: Sequence[VFolderMount],
        resource_spec: KernelResourceSpec,
    ) -> None:
        for vfolder in vfolders:
            if self.internal_data.get("prevent_vfolder_mounts", False):
                if vfolder.name != ".logs":
                    continue
            mount = Mount(
                MountTypes.BIND,
                Path(vfolder.host_path),
                Path(vfolder.kernel_path),
                vfolder.mount_perm,
            )
            resource_spec.mounts.append(mount)

    async def mount_krunner(
        self,
        resource_spec: KernelResourceSpec,
        environ: MutableMapping[str, str],
    ) -> None:
        def _mount(
            type,
            src,
            dst,
        ):
            resource_spec.mounts.append(
                self.get_runner_mount(
                    type,
                    src,
                    dst,
                    MountPermission("ro"),
                ),
            )

        distro = self.distro

        (
            arch,
            matched_distro,
            matched_libc_style,
            krunner_volume,
            krunner_pyver,
        ) = self.get_krunner_info()
        artifact_path = Path(pkg_resources.resource_filename("backend.agent", "../runner"))

        def find_artifacts(pattern: str) -> Mapping[str, str]:
            artifacts = {}
            for p in artifact_path.glob(pattern):
                m = self._rx_distro.search(p.name)
                if m is not None:
                    artifacts[m.group(1)] = p.name
            return artifacts

        def mount_versioned_binary(candidate_glob: str, target_path: str) -> None:
            candidates = find_artifacts(candidate_glob)
            _, candidate = match_distro_data(candidates, distro)
            resolved_path = self.resolve_krunner_filepath("runner/" + candidate)
            _mount(MountTypes.BIND, resolved_path, target_path)

        def mount_static_binary(filename: str, target_path: str) -> None:
            resolved_path = self.resolve_krunner_filepath("runner/" + filename)
            _mount(MountTypes.BIND, resolved_path, target_path)

        mount_static_binary(f"su-exec.{arch}.bin", "/opt/kernel/su-exec")
        mount_versioned_binary(f"libbaihook.*.{arch}.so", "/opt/kernel/libbaihook.so")
        mount_static_binary(f"dropbearmulti.{arch}.bin", "/opt/kernel/dropbearmulti")
        mount_static_binary(f"sftp-server.{arch}.bin", "/opt/kernel/sftp-server")
        mount_static_binary(f"tmux.{arch}.bin", "/opt/kernel/tmux")

        jail_path: Optional[Path]
        if self.local_config["container"]["sandbox-type"] == "jail":
            jail_candidates = find_artifacts(
                f"jail.*.{arch}.bin"
            )
            _, jail_candidate = match_distro_data(jail_candidates, distro)
            jail_path = self.resolve_krunner_filepath("runner/" + jail_candidate)
        else:
            jail_path = None

        dotfile_extractor_path = self.resolve_krunner_filepath("runner/extract_dotfiles.py")
        persistent_files_warning_doc_path = self.resolve_krunner_filepath(
            "runner/DO_NOT_STORE_PERSISTENT_FILES_HERE.md"
        )
        entrypoint_sh_path = self.resolve_krunner_filepath("runner/entrypoint.sh")

        fantompass_path = self.resolve_krunner_filepath("runner/fantompass.py")
        hash_phrase_path = self.resolve_krunner_filepath("runner/hash_phrase.py")
        words_json_path = self.resolve_krunner_filepath("runner/words.json")

        if matched_libc_style == "musl":
            terminfo_path = self.resolve_krunner_filepath("runner/terminfo.alpine3.8")
            _mount(MountTypes.BIND, terminfo_path, "/home/work/.terminfo")

        _mount(MountTypes.BIND, dotfile_extractor_path, "/opt/kernel/extract_dotfiles.py")
        _mount(MountTypes.BIND, entrypoint_sh_path, "/opt/kernel/entrypoint.sh")
        _mount(MountTypes.BIND, fantompass_path, "/opt/kernel/fantompass.py")
        _mount(MountTypes.BIND, hash_phrase_path, "/opt/kernel/hash_phrase.py")
        _mount(MountTypes.BIND, words_json_path, "/opt/kernel/words.json")
        if jail_path is not None:
            _mount(MountTypes.BIND, jail_path, "/opt/kernel/jail")
        _mount(
            MountTypes.BIND,
            persistent_files_warning_doc_path,
            "/home/work/DO_NOT_STORE_PERSISTENT_FILES_HERE.md",
        )

        _mount(MountTypes.VOLUME, krunner_volume, "/opt/backend")
        pylib_path = f"/opt/backend/lib/python{krunner_pyver}/site-packages/"
        kernel_pkg_path = self.resolve_krunner_filepath("kernel")
        helpers_pkg_path = self.resolve_krunner_filepath("helpers")
        _mount(MountTypes.BIND, kernel_pkg_path, pylib_path + "ai/backend/kernel")
        _mount(MountTypes.BIND, helpers_pkg_path, pylib_path + "ai/backend/helpers")
        environ["LD_PRELOAD"] = "/opt/kernel/libbaihook.so"

        already_injected_hooks: Set[Path] = set()
        for dev_type, device_alloc in resource_spec.allocations.items():
            computer_ctx = self.computers[dev_type]
            await self.apply_accelerator_allocation(
                computer_ctx.instance,
                device_alloc,
            )
            accelerator_mounts = await self.generate_accelerator_mounts(
                computer_ctx.instance,
                device_alloc,
            )
            for mount_info in accelerator_mounts:
                _mount(mount_info.mode, mount_info.src_path, mount_info.dst_path.as_posix())
            alloc_sum = Decimal(0)
            for dev_id, per_dev_alloc in device_alloc.items():
                alloc_sum += sum(per_dev_alloc.values())
            if alloc_sum > 0:
                hook_paths = await computer_ctx.instance.get_hooks(distro, arch)
                if hook_paths:
                    log.debug(
                        "accelerator {} provides hooks: {}",
                        type(computer_ctx.instance).__name__,
                        ", ".join(map(str, hook_paths)),
                    )
                for hook_path in map(lambda p: Path(p).absolute(), hook_paths):
                    if hook_path in already_injected_hooks:
                        continue
                    container_hook_path = f"/opt/kernel/{hook_path.name}"
                    _mount(MountTypes.BIND, hook_path, container_hook_path)
                    environ["LD_PRELOAD"] += ":" + container_hook_path
                    already_injected_hooks.add(hook_path)


    KernelCreationContextType = TypeVar(
        "KernelCreationContextType", bound=AbstractKernelCreationContext
    )


    @attrs.define(auto_attribs=True, slots=True)
    class RestartTracker:
        request_lock: asyncio.Lock
        destroy_event: asyncio.Event
        done_event: asyncio.Event


    @attrs.define(auto_attribs=True, slots=True)
    class ComputerContext:
        instance: AbstractComputePlugin
        devices: Collection[AbstractComputeDevice]
        alloc_map: AbstractAllocMap

    def __init__(
        self,
        etcd: AsyncEtcd,
        local_config: Mapping[str, Any],
        *,
        stats_monitor: StatsPluginContext,
        error_monitor: ErrorPluginContext,
        skip_initial_scan: bool = False,
        agent_public_key: Optional[PublicKey],
    ) -> None:
        self._skip_initial_scan = skip_initial_scan
        self.loop = current_loop()
        self.etcd = etcd
        self.local_config = local_config
        self.id = AgentId(local_config["agent"]["id"])
        self.local_instance_id = generate_local_instance_id(__file__)
        self.agent_public_key = agent_public_key
        self.kernel_registry = {}
        self.computers = {}
        self.images = {}
        self.restarting_kernels = {}
        self.stat_ctx = StatContext(
            self,
            mode=StatModes(local_config["container"]["stats-type"]),
        )
        self.timer_tasks = []
        self.port_pool = set(
            range(
                local_config["container"]["port-range"][0],
                local_config["container"]["port-range"][1] + 1,
            )
        )
        self.stats_monitor = stats_monitor
        self.error_monitor = error_monitor
        self._pending_creation_tasks = defaultdict(set)
        self._ongoing_exec_batch_tasks = weakref.WeakSet()
        self._ongoing_destruction_tasks = weakref.WeakValueDictionary()

            async def __ainit__(self) -> None:

        self.resource_lock = asyncio.Lock()
        self.registry_lock = asyncio.Lock()
        self.container_lifecycle_queue = asyncio.Queue()

        event_dispatcher_cls: type[EventDispatcher] | type[ExperimentalEventDispatcher]
        if self.local_config["agent"].get("use-experimental-redis-event-dispatcher"):
            event_dispatcher_cls = ExperimentalEventDispatcher
        else:
            event_dispatcher_cls = EventDispatcher

        self.event_producer = await EventProducer.new(
            self.local_config["redis"],
            db=REDIS_STREAM_DB,
            log_events=self.local_config["debug"]["log-events"],
        )
        self.event_dispatcher = await event_dispatcher_cls.new(
            self.local_config["redis"],
            db=REDIS_STREAM_DB,
            log_events=self.local_config["debug"]["log-events"],
            node_id=self.local_config["agent"]["id"],
            consumer_group=EVENT_DISPATCHER_CONSUMER_GROUP,
        )
        self.redis_stream_pool = redis_helper.get_redis_object(
            self.local_config["redis"],
            name="stream",
            db=REDIS_STREAM_DB,
        )
        self.redis_stat_pool = redis_helper.get_redis_object(
            self.local_config["redis"],
            name="stat",
            db=REDIS_STAT_DB,
        )

        self.background_task_manager = BackgroundTaskManager(self.event_producer)