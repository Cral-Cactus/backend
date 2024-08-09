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

    @abstractmethod
    async def load_resources(
        self,
    ) -> Mapping[DeviceName, AbstractComputePlugin]:

    @abstractmethod
    async def scan_available_resources(
        self,
    ) -> Mapping[SlotName, Decimal]:

    async def update_slots(
        self,
        interval: float,
    ) -> None:
        self.slots = await self.scan_available_resources()
        log.debug("slots: {!r}", self.slots)

    async def gather_hwinfo(self) -> Mapping[str, HardwareMetadata]:
        hwinfo: Dict[str, HardwareMetadata] = {}
        tasks: list[Awaitable[tuple[DeviceName, Exception | HardwareMetadata]]] = []

        async def _get(
            key: DeviceName,
            plugin: AbstractComputePlugin,
        ) -> tuple[DeviceName, Exception | HardwareMetadata]:
            try:
                result = await plugin.get_node_hwinfo()
                return key, result
            except Exception as e:
                return key, e

        for device_name, plugin in self.computers.items():
            tasks.append(_get(device_name, plugin.instance))
        results = await asyncio.gather(*tasks)
        for device_name, result in results:
            match result:
                case NotImplementedError():
                    continue
                case Exception():
                    hwinfo[device_name] = {
                        "status": "unavailable",
                        "status_info": str(result),
                        "metadata": {},
                    }
                case dict():
                    hwinfo[device_name] = result
        return hwinfo

    async def _cleanup_reported_kernels(self, interval: float):
        dest_path: Path = self.local_config["agent"]["abuse-report-path"]
        auto_terminate: bool = self.local_config["agent"].get(
            "force-terminate-abusing-containers", False
        )

        def _read(path: Path) -> str:
            with open(path, "r") as fr:
                return fr.read()

        def _rm(path: Path) -> None:
            os.remove(path)

        terminated_kernels: dict[str, ContainerLifecycleEvent] = {}
        abuse_report: dict[str, str] = {}
        try:
            async with FileLock(path=dest_path / "report.lock"):
                for reported_kernel in dest_path.glob("report.*.json"):
                    raw_body = await self.loop.run_in_executor(None, _read, reported_kernel)
                    body: dict[str, str] = json.loads(raw_body)
                    kern_id = body["ID"]
                    if auto_terminate:
                        log.debug("cleanup requested: {} ({})", body["ID"], body.get("reason"))
                        kernel_id = KernelId(UUID(body["ID"]))
                        kernel_obj = self.kernel_registry.get(kernel_id)
                        if kernel_obj is None:
                            continue
                        abuse_report[kern_id] = AbuseReportValue.CLEANING.value
                        session_id = kernel_obj.session_id
                        terminated_kernels[body["ID"]] = ContainerLifecycleEvent(
                            kernel_id,
                            session_id,
                            ContainerId(body["CID"]),
                            LifecycleEvent.DESTROY,
                            KernelLifecycleEventReason.from_value(body.get("reason"))
                            or KernelLifecycleEventReason.ANOMALY_DETECTED,
                        )
                        await self.loop.run_in_executor(None, _rm, reported_kernel)
                    else:
                        abuse_report[kern_id] = AbuseReportValue.DETECTED.value
                        log.debug(
                            "abusing container detected, skipping auto-termination: {} ({})",
                            kern_id,
                            body.get("reason"),
                        )
        except Exception:
            log.exception("error while paring abuse reports:")
        finally:
            for kid, ev in terminated_kernels.items():
                await self.container_lifecycle_queue.put(ev)

            hash_name = "abuse_report"
            abuse_report_script = textwrap.dedent(
            )
            await redis_helper.execute_script(
                self.redis_stat_pool,
                "report_abusing_kernels",
                abuse_report_script,
                [hash_name],
                [json.dumps(abuse_report)],
            )

    @abstractmethod
    async def scan_images(self) -> Mapping[str, str]:

    async def _scan_images_wrapper(self, interval: float) -> None:
        self.images = await self.scan_images()

    @abstractmethod
    async def push_image(self, image_ref: ImageRef, registry_conf: ImageRegistry) -> None:

    @abstractmethod
    async def pull_image(self, image_ref: ImageRef, registry_conf: ImageRegistry) -> None:

    @abstractmethod
    async def check_image(
        self, image_ref: ImageRef, image_id: str, auto_pull: AutoPullBehavior
    ) -> bool:
        return False

    async def scan_running_kernels(self) -> None:
        ipc_base_path = self.local_config["agent"]["ipc-base-path"]
        var_base_path = self.local_config["agent"]["var-base-path"]
        last_registry_file = f"last_registry.{self.local_instance_id}.dat"
        if os.path.isfile(ipc_base_path / last_registry_file):
            shutil.move(ipc_base_path / last_registry_file, var_base_path / last_registry_file)
        try:
            with open(var_base_path / last_registry_file, "rb") as f:
                self.kernel_registry = pickle.load(f)
        except EOFError:
            log.warning(
                "Failed to load the last kernel registry: {}", (var_base_path / last_registry_file)
            )
        except FileNotFoundError:
            pass
        for kernel_obj in self.kernel_registry.values():
            kernel_obj.agent_config = self.local_config
            if kernel_obj.runner is not None:
                kernel_obj.runner.event_producer = self.event_producer
                await kernel_obj.runner.__ainit__()
        async with self.registry_lock:
            for kernel_id, container in await self.enumerate_containers(
                ACTIVE_STATUS_SET | DEAD_STATUS_SET,
            ):
                session_id = SessionId(UUID(container.labels["ai.backend.session-id"]))
                if container.status in ACTIVE_STATUS_SET:
                    kernelspec = int(container.labels.get("ai.backend.kernelspec", "1"))
                    if not (MIN_KERNELSPEC <= kernelspec <= MAX_KERNELSPEC):
                        continue
                    for p in container.ports:
                        if p.host_port is not None:
                            self.port_pool.discard(p.host_port)
                    async with self.resource_lock:
                        for computer_ctx in self.computers.values():
                            await computer_ctx.instance.restore_from_container(
                                container,
                                computer_ctx.alloc_map,
                            )
                    await self.inject_container_lifecycle_event(
                        kernel_id,
                        session_id,
                        LifecycleEvent.START,
                        KernelLifecycleEventReason.RESUMING_AGENT_OPERATION,
                        container_id=container.id,
                    )
                elif container.status in DEAD_STATUS_SET:
                    log.info(
                        "detected dead container while agent is down (k:{}, c:{})",
                        kernel_id,
                        container.id,
                    )
                    await self.inject_container_lifecycle_event(
                        kernel_id,
                        session_id,
                        LifecycleEvent.CLEAN,
                        KernelLifecycleEventReason.SELF_TERMINATED,
                        container_id=container.id,
                    )

        log.info("starting with resource allocations")
        for computer_name, computer_ctx in self.computers.items():
            log.info("{}: {!r}", computer_name, dict(computer_ctx.alloc_map.allocations))

    @abstractmethod
    async def init_kernel_context(
        self,
        kernel_id: KernelId,
        session_id: SessionId,
        kernel_config: KernelCreationConfig,
        *,
        restarting: bool = False,
        cluster_ssh_port_mapping: Optional[ClusterSSHPortMapping] = None,
    ) -> AbstractKernelCreationContext:
        raise NotImplementedError

        async def create_batch_execution_task(
        self,
        session_id: SessionId,
        kernel_id: KernelId,
        code_to_execute: str,
    ) -> None:
        self._ongoing_exec_batch_tasks.add(
            asyncio.create_task(
                self.execute_batch(session_id, kernel_id, code_to_execute),
            ),
        )

    async def create_kernel(
        self,
        session_id: SessionId,
        kernel_id: KernelId,
        kernel_config: KernelCreationConfig,
        cluster_info: ClusterInfo,
        *,
        restarting: bool = False,
        throttle_sema: Optional[asyncio.Semaphore] = None,
    ) -> KernelCreationResult:
        if throttle_sema is None:
            throttle_sema = asyncio.Semaphore(1)
        async with throttle_sema:
            if not restarting:
                await self.produce_event(
                    KernelPreparingEvent(kernel_id, session_id),
                )

            if self.local_config["debug"]["log-kernel-config"]:
                log.debug("Kernel creation config: {0}", pretty(kernel_config))
            ctx = await self.init_kernel_context(
                kernel_id,
                session_id,
                kernel_config,
                restarting=restarting,
                cluster_ssh_port_mapping=cluster_info.get("cluster_ssh_port_mapping"),
            )
            environ: MutableMapping[str, str] = {**kernel_config["environ"]}

            if KernelFeatures.UID_MATCH in ctx.kernel_features:
                uid = self.local_config["container"]["kernel-uid"]
                gid = self.local_config["container"]["kernel-gid"]
                environ["LOCAL_USER_ID"] = str(uid)
                environ["LOCAL_GROUP_ID"] = str(gid)
            environ.update(
                await ctx.get_extra_envs(),
            )
            image_labels = kernel_config["image"]["labels"]

            agent_architecture = get_arch_name()
            if agent_architecture != ctx.image_ref.architecture:
                raise AgentError(
                    f"cannot run {ctx.image_ref.architecture} image on"
                    f" {agent_architecture} machine",
                )

            do_pull = (not ctx.image_ref.is_local) and await self.check_image(
                ctx.image_ref,
                kernel_config["image"]["digest"],
                AutoPullBehavior(kernel_config.get("auto_pull", "digest")),
            )
            if do_pull:
                await self.produce_event(
                    KernelPullingEvent(kernel_id, session_id, ctx.image_ref.canonical),
                )
                await self.pull_image(ctx.image_ref, kernel_config["image"]["registry"])

            if not restarting:
                await self.produce_event(
                    KernelCreatingEvent(kernel_id, session_id),
                )

            if not restarting:
                resource_spec.mounts.extend(
                    await ctx.get_intrinsic_mounts(),
                )

            if not restarting:
                alloc_order = [
                    DeviceName(name) for name in self.local_config["resource"]["allocation-order"]
                ]
                async with self.resource_lock:
                    try:
                        allocate(
                            self.computers,
                            resource_spec,
                            alloc_order,
                            self.affinity_map,
                            self.local_config["resource"]["affinity-policy"],
                        )
                    except ResourceError:
                        await self.produce_event(DoAgentResourceCheckEvent(ctx.agent_id))
                        raise
            try:
                if not restarting:
                    await ctx.prepare_scratch()

                await ctx.apply_network(cluster_info)
                await ctx.prepare_ssh(cluster_info)

                vfolder_mounts = [VFolderMount.from_json(item) for item in kernel_config["mounts"]]
                if not restarting:
                    await ctx.mount_vfolders(vfolder_mounts, resource_spec)
                    await ctx.mount_krunner(resource_spec, environ)

                label_envs_corecount = image_labels.get("backend.envs.corecount", "")
                envs_corecount = label_envs_corecount.split(",") if label_envs_corecount else []
                cpu_core_count = len(resource_spec.allocations[DeviceName("cpu")][SlotName("cpu")])
                environ.update({k: str(cpu_core_count) for k in envs_corecount if k not in environ})

                await ctx.process_mounts(resource_spec.mounts)

                attached_devices = {}
                for dev_name, device_alloc in resource_spec.allocations.items():
                    computer_ctx = self.computers[dev_name]
                    devices = await computer_ctx.instance.get_attached_devices(device_alloc)
                    attached_devices[dev_name] = devices

                exposed_ports = [2000, 2001]
                service_ports: List[ServicePort] = []
                port_map: Dict[str, ServicePort] = {}
                preopen_ports = ctx.kernel_config.get("preopen_ports")
                if preopen_ports is None:
                    preopen_ports = []

                service_ports.append({
                    "name": "sshd",
                    "protocol": ServicePortProtocols.TCP,
                    "container_ports": (2200,),
                    "host_ports": (None,),
                    "is_inference": False,
                })
                service_ports.append({
                    "name": "ttyd",
                    "protocol": ServicePortProtocols.HTTP,
                    "container_ports": (7681,),
                    "host_ports": (None,),
                    "is_inference": False,
                })

                model_definition: Optional[Mapping[str, Any]] = None
                model_folders = [
                    folder
                    for folder in vfolder_mounts
                    if folder.usage_mode == VFolderUsageMode.MODEL
                ]

                if ctx.kernel_config["cluster_role"] in ("main", "master"):
                    for sport in parse_service_ports(
                        image_labels.get("backend.service-ports", ""),
                        image_labels.get("backend.endpoint-ports", ""),
                    ):
                        port_map[sport["name"]] = sport
                    for port_no in preopen_ports:
                        if port_no in (2000, 2001):
                            raise AgentError("Port 2000 and 2001 are reserved for internal use")
                        overlapping_services = [
                            s for s in service_ports if port_no in s["container_ports"]
                        ]
                        if len(overlapping_services) > 0:
                            raise AgentError(
                                f"Port {port_no} overlaps with built-in service"
                                f" {overlapping_services[0]['name']}"
                            )

                        preopen_sport: ServicePort = {
                            "name": str(port_no),
                            "protocol": ServicePortProtocols.PREOPEN,
                            "container_ports": (port_no,),
                            "host_ports": (None,),
                            "is_inference": False,
                        }
                        service_ports.append(preopen_sport)
                        for cport in preopen_sport["container_ports"]:
                            exposed_ports.append(cport)
                    for sport in port_map.values():
                        service_ports.append(sport)
                        for cport in sport["container_ports"]:
                            exposed_ports.append(cport)
                    for index, port in enumerate(ctx.kernel_config["allocated_host_ports"]):
                        service_ports.append({
                            "name": f"hostport{index+1}",
                            "protocol": ServicePortProtocols.INTERNAL,
                            "container_ports": (port,),
                            "host_ports": (port,),
                            "is_inference": False,
                        })
                        exposed_ports.append(port)
                    log.debug("exposed ports: {!r}", exposed_ports)
                                    if kernel_config["session_type"] == SessionTypes.INFERENCE:
                    model_definition = await self.load_model_definition(
                        RuntimeVariant(
                            (kernel_config["internal_data"] or {}).get("runtime_variant", "custom")
                        ),
                        model_folders,
                        environ,
                        service_ports,
                        kernel_config,
                    )

                runtime_type = image_labels.get("backend.runtime-type", "app")
                runtime_path = image_labels.get("backend.runtime-path", None)
                cmdargs: list[str] = []
                krunner_opts: list[str] = []
                if self.local_config["container"]["sandbox-type"] == "jail":
                    cmdargs += [
                        "/opt/kernel/jail",
                    ]
                    if self.local_config["container"]["jail-args"]:
                        cmdargs += map(
                            lambda s: s.strip(), self.local_config["container"]["jail-args"]
                        )
                    cmdargs += ["--"]
                if self.local_config["debug"]["kernel-runner"]:
                    krunner_opts.append("--debug")
                cmdargs += [
                    "/opt/backend/bin/python",
                    "-s",
                    "-m",
                    "backend.kernel",
                    *krunner_opts,
                    runtime_type,
                ]
                if runtime_path is not None:
                    cmdargs.append(runtime_path)

                resource_spec.freeze()
                await self.restart_kernel__store_config(
                    kernel_id,
                    "kconfig.dat",
                    pickle.dumps(ctx.kernel_config),
                )