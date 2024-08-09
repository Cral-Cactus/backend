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

                alloc_map_mod.log_alloc_map = self.local_config["debug"]["log-alloc-map"]
        computers = await self.load_resources()

        all_devices: List[AbstractComputeDevice] = []
        metadatas: List[AcceleratorMetadata] = []
        for name, computer in computers.items():
            devices = await computer.list_devices()
            all_devices.extend(devices)
            alloc_map = await computer.create_alloc_map()
            self.computers[name] = ComputerContext(computer, devices, alloc_map)
            metadatas.append(computer.get_metadata())

        self.slots = await self.scan_available_resources()
        log.info("Resource slots: {!r}", self.slots)
        log.info("Slot types: {!r}", known_slot_types)
        self.timer_tasks.append(aiotools.create_timer(self.update_slots, 30.0))

        async def _pipeline(r: Redis):
            pipe = r.pipeline()
            for metadata in metadatas:
                await pipe.hset(
                    "computer.metadata",
                    metadata["slot_name"],
                    json.dumps(metadata),
                )
            return pipe

        await redis_helper.execute(self.redis_stat_pool, _pipeline)

        self.affinity_map = AffinityMap.build(all_devices)

        if not self._skip_initial_scan:
            self.images = await self.scan_images()
            self.timer_tasks.append(aiotools.create_timer(self._scan_images_wrapper, 20.0))
            await self.scan_running_kernels()

        self.timer_tasks.append(aiotools.create_timer(self.collect_node_stat, 5.0))
        self.timer_tasks.append(aiotools.create_timer(self.collect_container_stat, 5.0))
        self.timer_tasks.append(aiotools.create_timer(self.collect_process_stat, 5.0))

        heartbeat_interval = self.local_config["debug"]["heartbeat-interval"]
        self.timer_tasks.append(aiotools.create_timer(self.heartbeat, heartbeat_interval))

        sync_container_lifecycles_config = self.local_config["agent"]["sync-container-lifecycles"]
        if sync_container_lifecycles_config["enabled"]:
            self.timer_tasks.append(
                aiotools.create_timer(
                    self.sync_container_lifecycles, sync_container_lifecycles_config["interval"]
                )
            )

        if abuse_report_path := self.local_config["agent"].get("abuse-report-path"):
            log.info(
                "Monitoring abnormal kernel activities reported by Watcher at {}", abuse_report_path
            )
            abuse_report_path.mkdir(exist_ok=True, parents=True)
            self.timer_tasks.append(aiotools.create_timer(self._cleanup_reported_kernels, 30.0))

        self.timer_tasks.append(
            aiotools.create_timer(self._report_all_kernel_commit_status_map, 7.0)
        )

        loop = current_loop()
        self.last_registry_written_time = time.monotonic()
        self.container_lifecycle_handler = loop.create_task(self.process_lifecycle_events())

        await self.produce_event(AgentStartedEvent(reason="self-started"))

        evd = self.event_dispatcher
        evd.subscribe(DoVolumeMountEvent, self, handle_volume_mount, name="ag.volume.mount")
        evd.subscribe(DoVolumeUnmountEvent, self, handle_volume_umount, name="ag.volume.umount")

    async def shutdown(self, stop_signal: signal.Signals) -> None:

        await cancel_tasks(self._ongoing_exec_batch_tasks)

        async with self.registry_lock:
            for kernel_obj in self.kernel_registry.values():
                if kernel_obj.runner is not None:
                    await kernel_obj.runner.close()
                await kernel_obj.close()
            await self.save_last_registry(force=True)
            if stop_signal == signal.SIGTERM:
                await self.clean_all_kernels(blocking=True)

        cancel_results = await cancel_tasks(self.timer_tasks)
        for result in cancel_results:
            if isinstance(result, Exception):
                log.error("timer cancellation error: {}", result)

        await self.container_lifecycle_queue.put(_sentinel)
        await self.container_lifecycle_handler

        await self.produce_event(AgentTerminatedEvent(reason="shutdown"))

        await self.event_producer.close()
        await self.event_dispatcher.close()
        await self.redis_stream_pool.close()
        await self.redis_stat_pool.close()

    async def produce_event(self, event: AbstractEvent) -> None:
        if self.local_config["debug"]["log-heartbeats"]:
            _log = log.debug if isinstance(event, AgentHeartbeatEvent) else log.info
        else:
            _log = (lambda *args: None) if isinstance(event, AgentHeartbeatEvent) else log.info
        if self.local_config["debug"]["log-events"]:
            _log("produce_event({0})", event)
        if isinstance(event, KernelTerminatedEvent):
            pending_creation_tasks = self._pending_creation_tasks.get(event.kernel_id, None)
            if pending_creation_tasks is not None:
                for t in set(pending_creation_tasks):
                    if not t.done() and not t.cancelled():
                        t.cancel()
                        try:
                            await t
                        except asyncio.CancelledError:
                            continue
        if isinstance(event, KernelStartedEvent) or isinstance(event, KernelTerminatedEvent):
            await self.save_last_registry()
        await self.event_producer.produce_event(event, source=str(self.id))

    async def produce_error_event(
        self,
        exc_info: Tuple[Type[BaseException], BaseException, TracebackType] = None,
    ) -> None:
        exc_type, exc, tb = sys.exc_info() if exc_info is None else exc_info
        pretty_message = "".join(traceback.format_exception_only(exc_type, exc)).strip()
        pretty_tb = "".join(traceback.format_tb(tb)).strip()
        await self.produce_event(AgentErrorEvent(pretty_message, pretty_tb))

    async def _report_all_kernel_commit_status_map(self, interval: float) -> None:
        loop = current_loop()
        base_commit_path: Path = self.local_config["agent"]["image-commit-path"]
        commit_kernels: set[str] = set()

        def _map_commit_status() -> None:
            for subdir in base_commit_path.iterdir():
                for commit_path in subdir.glob("./**/lock/*"):
                    kern = commit_path.name
                    if kern not in commit_kernels:
                        commit_kernels.add(kern)

        await loop.run_in_executor(None, _map_commit_status)

        commit_status_script = textwrap.dedent(
        )
        await redis_helper.execute_script(
            self.redis_stat_pool,
            "check_kernel_commit_statuses",
            commit_status_script,
            [f"kernel.{kern}.commit" for kern in commit_kernels],
            [COMMIT_STATUS_EXPIRE],
        )

    async def heartbeat(self, interval: float):
        res_slots = {}
        try:
            for cctx in self.computers.values():
                for slot_key, slot_type in cctx.instance.slot_types:
                    res_slots[slot_key] = (
                        slot_type,
                        str(self.slots.get(slot_key, 0)),
                    )
            if self.local_config["agent"]["advertised-rpc-addr"]:
                rpc_addr = self.local_config["agent"]["advertised-rpc-addr"]
            else:
                rpc_addr = self.local_config["agent"]["rpc-listen-addr"]
            agent_info = {
                "ip": str(rpc_addr.host),
                "region": self.local_config["agent"]["region"],
                "scaling_group": self.local_config["agent"]["scaling-group"],
                "addr": f"tcp://{rpc_addr}",
                "public_key": self.agent_public_key,
                "public_host": str(self._get_public_host()),
                "resource_slots": res_slots,
                "version": VERSION,
                "compute_plugins": {
                    key: {
                        "version": computer.instance.get_version(),
                        **(await computer.instance.extra_info()),
                    }
                    for key, computer in self.computers.items()
                },
                "images": zlib.compress(
                    msgpack.packb([(repo_tag, digest) for repo_tag, digest in self.images.items()])
                ),
                "images.opts": {"compression": "zlib"},
                "architecture": get_arch_name(),
                "auto_terminate_abusing_kernel": self.local_config["agent"][
                    "force-terminate-abusing-containers"
                ],
            }
            await self.produce_event(AgentHeartbeatEvent(agent_info))
        except asyncio.TimeoutError:
            log.warning("event dispatch timeout: instance_heartbeat")
        except Exception:
            log.exception("instance_heartbeat failure")
            await self.produce_error_event()

            async def collect_logs(
        self,
        kernel_id: KernelId,
        container_id: str,
        async_log_iterator: AsyncIterator[bytes],
    ) 

        async def collect_node_stat(self, interval: float):
        if self.local_config["debug"]["log-stats"]:
            log.debug("collecting node statistics")
        try:
            await self.stat_ctx.collect_node_stat()
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("unhandled exception while syncing node stats")
            await self.produce_error_event()

    async def collect_container_stat(self, interval: float):
        if self.local_config["debug"]["log-stats"]:
            log.debug("collecting container statistics")
        try:
            container_ids = []
            async with self.registry_lock:
                for kernel_id, kernel_obj in [*self.kernel_registry.items()]:
                    if (
                        not kernel_obj.stats_enabled
                        or kernel_obj.state != KernelLifecycleStatus.RUNNING
                    ):
                        continue
                    container_ids.append(kernel_obj["container_id"])
                await self.stat_ctx.collect_container_stat(container_ids)
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("unhandled exception while syncing container stats")
            await self.produce_error_event()

    async def collect_process_stat(self, interval: float):
        if self.local_config["debug"]["log-stats"]:
            log.debug("collecting process statistics in container")
        try:
            updated_kernel_ids = []
            container_ids = []
            async with self.registry_lock:
                for kernel_id, kernel_obj in [*self.kernel_registry.items()]:
                    if (
                        not kernel_obj.stats_enabled
                        or kernel_obj.state != KernelLifecycleStatus.RUNNING
                    ):
                        continue
                    updated_kernel_ids.append(kernel_id)
                    container_ids.append(kernel_obj["container_id"])
                await self.stat_ctx.collect_per_container_process_stat(container_ids)
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("unhandled exception while syncing process stats")
            await self.produce_error_event()

    def _get_public_host(self) -> str:
        agent_config: Mapping[str, Any] = self.local_config["agent"]
        container_config: Mapping[str, Any] = self.local_config["container"]
        return (
            agent_config.get("public-host")
            or container_config.get("advertised-host")
            or container_config["bind-host"]
        )

    async def _handle_start_event(self, ev: ContainerLifecycleEvent) -> None:
        async with self.registry_lock:
            kernel_obj = self.kernel_registry.get(ev.kernel_id)
            if kernel_obj is not None:
                kernel_obj.stats_enabled = True
                kernel_obj.state = KernelLifecycleStatus.RUNNING

    async def _handle_destroy_event(self, ev: ContainerLifecycleEvent) -> None:
        try:
            current_task = asyncio.current_task()
            assert current_task is not None
            if ev.kernel_id not in self._ongoing_destruction_tasks:
                self._ongoing_destruction_tasks[ev.kernel_id] = current_task
            async with self.registry_lock:
                kernel_obj = self.kernel_registry.get(ev.kernel_id)
                if kernel_obj is None:
                    log.warning(
                        "destroy_kernel(k:{0}, c:{1}) kernel missing (already dead?)",
                        ev.kernel_id,
                        ev.container_id,
                    )
                    if ev.container_id is None:
                        await self.reconstruct_resource_usage()
                        if not ev.suppress_events:
                            await self.produce_event(
                                KernelTerminatedEvent(
                                    ev.kernel_id,
                                    ev.session_id,
                                    reason=KernelLifecycleEventReason.ALREADY_TERMINATED,
                                ),
                            )
                        if ev.done_future is not None:
                            ev.done_future.set_result(None)
                        return
                else:
                    kernel_obj.state = KernelLifecycleStatus.TERMINATING
                    kernel_obj.stats_enabled = False
                    kernel_obj.termination_reason = ev.reason
                    if kernel_obj.runner is not None:
                        await kernel_obj.runner.close()
                    kernel_obj.clean_event = ev.done_future
                try:
                    await self.destroy_kernel(ev.kernel_id, ev.container_id)
                except Exception as e:
                    if ev.done_future is not None:
                        ev.done_future.set_exception(e)
                    raise
                finally:
                    await self.container_lifecycle_queue.put(
                        ContainerLifecycleEvent(
                            ev.kernel_id,
                            ev.session_id,
                            ev.container_id,
                            LifecycleEvent.CLEAN,
                            ev.reason,
                            suppress_events=ev.suppress_events,
                            done_future=ev.done_future,
                        ),
                    )
        except asyncio.CancelledError:
            pass
        except Exception:
            log.exception("unhandled exception while processing DESTROY event")
            await self.produce_error_event()

    async def _handle_clean_event(self, ev: ContainerLifecycleEvent) -> None:
        destruction_task = self._ongoing_destruction_tasks.get(ev.kernel_id, None)
        if destruction_task is not None and not destruction_task.done():
            await destruction_task
            del destruction_task
        async with self.registry_lock:
            try:
                kernel_obj = self.kernel_registry.get(ev.kernel_id)
                if kernel_obj is not None and kernel_obj.runner is not None:
                    await kernel_obj.runner.close()
                await self.clean_kernel(
                    ev.kernel_id,
                    ev.container_id,
                    ev.kernel_id in self.restarting_kernels,
                )
            except Exception as e:
                if ev.done_future is not None:
                    ev.done_future.set_exception(e)
                await self.produce_error_event()
            finally:
                if ev.kernel_id in self.restarting_kernels:
                    kernel_obj = self.kernel_registry.get(ev.kernel_id, None)
                else:
                    kernel_obj = self.kernel_registry.pop(ev.kernel_id, None)
                try:
                    if kernel_obj is not None:
                        port_range = self.local_config["container"]["port-range"]
                        if host_ports := kernel_obj.get("host_ports"):
                            restored_ports = [
                                *filter(
                                    lambda p: port_range[0] <= p <= port_range[1],
                                    host_ports,
                                )
                            ]
                            self.port_pool.update(restored_ports)
                        await kernel_obj.close()
                finally:
                    if restart_tracker := self.restarting_kernels.get(ev.kernel_id, None):
                        restart_tracker.destroy_event.set()
                    else:
                        await self.reconstruct_resource_usage()
                        if not ev.suppress_events:
                            await self.produce_event(
                                KernelTerminatedEvent(
                                    ev.kernel_id, ev.session_id, reason=ev.reason
                                ),
                            )
                    if kernel_obj is not None and kernel_obj.clean_event is not None:
                        kernel_obj.clean_event.set_result(None)
                    if ev.done_future is not None and not ev.done_future.done():
                        ev.done_future.set_result(None)

    async def inject_container_lifecycle_event(
        self,
        kernel_id: KernelId,
        session_id: SessionId,
        event: LifecycleEvent,
        reason: KernelLifecycleEventReason,
        *,
        container_id: Optional[ContainerId] = None,
        exit_code: int = None,
        done_future: asyncio.Future = None,
        suppress_events: bool = False,
    ) -> None:
        cid: Optional[ContainerId] = None
        try:
            kernel_obj = self.kernel_registry[kernel_id]
        except KeyError:
            if event == LifecycleEvent.START:
                pass
            else:
                log.warning(
                    "injecting lifecycle event (e:{}) for unknown kernel (k:{})",
                    event.name,
                    kernel_id,
                )
        else:
            assert kernel_obj is not None
            if kernel_obj.termination_reason:
                reason = kernel_obj.termination_reason
            if container_id is not None:
                if event == LifecycleEvent.START:
                    kernel_obj["container_id"] = container_id
                elif container_id != kernel_obj["container_id"]:
                    log.warning(
                        "container id mismatch for kernel_obj (k:{}, c:{}) with event (e:{}, c:{})",
                        kernel_id,
                        kernel_obj["container_id"],
                        event.name,
                        container_id,
                    )
            cid = kernel_obj.get("container_id")
        if cid is None:
            log.warning(
                "kernel has no container_id (k:{}) with event (e:{})",
                kernel_id,
                event.name,
            )
        await self.container_lifecycle_queue.put(
            ContainerLifecycleEvent(
                kernel_id,
                session_id,
                cid,
                event,
                reason,
                done_future,
                exit_code,
                suppress_events,
            ),
        )

    @abstractmethod
    async def resolve_image_distro(self, image: ImageConfig) -> str:
        raise NotImplementedError

    @abstractmethod
    async def enumerate_containers(
        self,
        status_filter: FrozenSet[ContainerStatus] = ACTIVE_STATUS_SET,
    ) -> Sequence[Tuple[KernelId, Container]]:

    async def reconstruct_resource_usage(self) -> None:
        async with self.resource_lock:
            for computer_ctx in self.computers.values():
                computer_ctx.alloc_map.clear()
            for kernel_id, container in await self.enumerate_containers():
                for computer_ctx in self.computers.values():
                    try:
                        await computer_ctx.instance.restore_from_container(
                            container,
                            computer_ctx.alloc_map,
                        )
                    except Exception:
                        log.warning(
                            "rescan_resource_usage(k:{}): "
                            "failed to read kernel resource info; "
                            "maybe already terminated",
                            kernel_id,
                        )

    async def sync_container_lifecycles(self, interval: float) -> None:
        known_kernels: Dict[KernelId, ContainerId | None] = {}
        alive_kernels: Dict[KernelId, ContainerId] = {}
        kernel_session_map: Dict[KernelId, SessionId] = {}
        own_kernels: dict[KernelId, ContainerId] = {}
        terminated_kernels: dict[KernelId, ContainerLifecycleEvent] = {}

        def _get_session_id(container: Container) -> SessionId | None:
            _session_id = container.labels.get("ai.backend.session-id")
            try:
                return SessionId(UUID(_session_id))
            except ValueError:
                log.warning(
                    f"sync_container_lifecycles() invalid session-id (cid: {container.id}, sid:{_session_id})"
                )
                return None

        log.debug("sync_container_lifecycles(): triggered")
        try:
            _containers = await self.enumerate_containers(ACTIVE_STATUS_SET | DEAD_STATUS_SET)
            async with self.registry_lock:
                try:
                    dead_containers = [
                        (kid, container)
                        for kid, container in _containers
                        if container.status in DEAD_STATUS_SET
                    ]
                    log.debug(
                        f"detected dead containers: {[container.id[:12] for _, container in dead_containers]}"
                    )
                    for kernel_id, container in dead_containers:
                        if kernel_id in self.restarting_kernels:
                            continue
                        log.info(
                            "detected dead container during lifeycle sync (k:{}, c:{})",
                            kernel_id,
                            container.id,
                        )
                        session_id = _get_session_id(container)
                        if session_id is None:
                            continue
                        terminated_kernels[kernel_id] = ContainerLifecycleEvent(
                            kernel_id,
                            session_id,
                            container.id,
                            LifecycleEvent.CLEAN,
                            KernelLifecycleEventReason.SELF_TERMINATED,
                        )
                    active_containers = [
                        (kid, container)
                        for kid, container in _containers
                        if container.status in ACTIVE_STATUS_SET
                    ]
                    log.debug(
                        f"detected active containers: {[container.id[:12] for _, container in active_containers]}"
                    )
                    for kernel_id, container in active_containers:
                        alive_kernels[kernel_id] = container.id
                        session_id = _get_session_id(container)
                        if session_id is None:
                            continue
                        kernel_session_map[kernel_id] = session_id
                        own_kernels[kernel_id] = container.id
                    for kernel_id, kernel_obj in self.kernel_registry.items():
                        known_kernels[kernel_id] = (
                            ContainerId(kernel_obj.container_id)
                            if kernel_obj.container_id is not None
                            else None
                        )
                        session_id = kernel_obj.session_id
                        kernel_session_map[kernel_id] = session_id
                    for kernel_id in known_kernels.keys() - alive_kernels.keys():
                        kernel_obj = self.kernel_registry[kernel_id]
                        if (
                            kernel_id in self.restarting_kernels
                            or kernel_obj.state != KernelLifecycleStatus.RUNNING
                        ):
                            continue
                        log.debug(f"kernel with no container (kid: {kernel_id})")
                        terminated_kernels[kernel_id] = ContainerLifecycleEvent(
                            kernel_id,
                            kernel_session_map[kernel_id],
                            known_kernels[kernel_id],
                            LifecycleEvent.CLEAN,
                            KernelLifecycleEventReason.CONTAINER_NOT_FOUND,
                        )
                    for kernel_id in alive_kernels.keys() - known_kernels.keys():
                        if kernel_id in self.restarting_kernels:
                            continue
                        log.debug(f"kernel not found in registry (kid:{kernel_id})")
                        terminated_kernels[kernel_id] = ContainerLifecycleEvent(
                            kernel_id,
                            kernel_session_map[kernel_id],
                            alive_kernels[kernel_id],
                            LifecycleEvent.DESTROY,
                            KernelLifecycleEventReason.TERMINATED_UNKNOWN_CONTAINER,
                        )
                finally:
                    terminated_kernel_ids = ",".join([
                        str(kid) for kid in terminated_kernels.keys()
                    ])
                    if terminated_kernel_ids:
                        log.debug(f"Terminate kernels(ids:[{terminated_kernel_ids}])")
                    for kernel_id, ev in terminated_kernels.items():
                        await self.container_lifecycle_queue.put(ev)

                    await self.set_container_count(len(own_kernels.keys()))
        except asyncio.CancelledError:
            pass
        except asyncio.TimeoutError:
            log.warning("sync_container_lifecycles() timeout, continuing")
        except Exception as e:
            log.exception(f"sync_container_lifecycles() failure, continuing (detail: {repr(e)})")
            await self.produce_error_event()

    async def set_container_count(self, container_count: int) -> None:
        await redis_helper.execute(
            self.redis_stat_pool, lambda r: r.set(f"container_count.{self.id}", container_count)
        )

    async def clean_all_kernels(self, blocking: bool = False) -> None:
        kernel_ids = [*self.kernel_registry.keys()]
        clean_events = {}
        loop = asyncio.get_running_loop()
        if blocking:
            for kernel_id in kernel_ids:
                clean_events[kernel_id] = loop.create_future()
        for kernel_id in kernel_ids:
            await self.inject_container_lifecycle_event(
                kernel_id,
                self.kernel_registry[kernel_id].session_id,
                LifecycleEvent.DESTROY,
                KernelLifecycleEventReason.AGENT_TERMINATION,
                done_future=clean_events[kernel_id] if blocking else None,
            )
        if blocking:
            waiters = [clean_events[kernel_id] for kernel_id in kernel_ids]
            await asyncio.gather(*waiters)