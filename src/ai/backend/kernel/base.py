from __future__ import annotations

import asyncio
import concurrent.futures
import enum
import json
import logging
import os
import resource
import signal
import sys
import time
import urllib.error
import urllib.request
import uuid
from abc import ABCMeta, abstractmethod
from functools import partial
from pathlib import Path
from typing import (
    Awaitable,
    ClassVar,
    Dict,
    List,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    TypeVar,
    Union,
)

import janus
import msgpack
import zmq
from jupyter_client.asynchronous.client import AsyncKernelClient
from jupyter_client.kernelspec import KernelSpecManager
from jupyter_client.manager import AsyncKernelManager

from .compat import current_loop
from .intrinsic import (
    init_sshd_service,
    prepare_sshd_service,
    prepare_ttyd_service,
)
from .jupyter_client import aexecute_interactive
from .logging import BraceStyleAdapter, setup_logger
from .service import ServiceParser
from .utils import TracebackSourceFilter, scan_proc_stats, wait_local_port_open

logger = logging.getLogger()
logger.addFilter(TracebackSourceFilter(str(Path(__file__).parent)))
log = BraceStyleAdapter(logger)

TReturn = TypeVar("TReturn")


class FailureSentinel(enum.Enum):
    TOKEN = 0


FAILURE = FailureSentinel.TOKEN


class HealthStatus(enum.Enum):
    HEALTHY = 0
    UNHEALTHY = 1
    UNDETERMINED = 2


async def pipe_output(stream, outsock, target, log_fd):
    assert target in ("stdout", "stderr")
    target = target.encode("ascii")
    console_fd = sys.stdout.fileno() if target == "stdout" else sys.stderr.fileno()
    loop = current_loop()
    try:
        while True:
            data = await stream.read(4096)
            if not data:
                break
            await asyncio.gather(
                loop.run_in_executor(None, os.write, console_fd, data),
                loop.run_in_executor(None, os.write, log_fd, data),
                outsock.send_multipart([target, data]),
                return_exceptions=True,
            )
    except asyncio.CancelledError:
        pass
    except Exception:
        log.exception("unexpected error")


async def terminate_and_wait(proc: asyncio.subprocess.Process, timeout: float = 2.0) -> None:
    try:
        proc.terminate()
        try:
            await asyncio.wait_for(proc.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.wait()
    except ProcessLookupError:
        pass


def glob_path(base: Path | str, pattern: str) -> Path | None:
    paths = [*Path(base).glob(pattern)]
    if paths:
        return paths[0]
    return None


def promote_path(path_env: str, path_to_promote: Path | str | None) -> str:
    if not path_to_promote:
        return path_env
    paths = path_env.split(":")
    path_to_promote = os.fsdecode(path_to_promote)
    result_paths = [p for p in paths if path_to_promote != p]
    result_paths.insert(0, path_to_promote)
    return ":".join(result_paths)


class BaseRunner(metaclass=ABCMeta):
    log_prefix: ClassVar[str] = "generic-kernel"
    log_queue: janus.Queue[logging.LogRecord]
    task_queue: asyncio.Queue[Awaitable[None]]
    default_runtime_path: ClassVar[Optional[str]] = None
    default_child_env: ClassVar[dict[str, str]] = {
        "LANG": "C.UTF-8",
        "HOME": "/home/work",
        "TERM": "xterm",
        "LD_LIBRARY_PATH": os.environ.get("LD_LIBRARY_PATH", ""),
        "LD_PRELOAD": os.environ.get("LD_PRELOAD", ""),
        "SSH_AUTH_SOCK": os.environ.get("SSH_AUTH_SOCK", ""),
        "SSH_AGENT_PID": os.environ.get("SSH_AGENT_PID", ""),
    }
    default_child_env_path = ":".join([
        "/usr/local/sbin",
        "/usr/local/bin",
        "/usr/sbin",
        "/usr/bin",
        "/sbin",
        "/bin",
    ])
    default_child_env_shell = "/bin/ash" if Path("/bin/ash").is_file() else "/bin/bash"
    jupyter_kspec_name: ClassVar[str] = ""
    kernel_mgr: Optional[AsyncKernelManager] = None
    kernel_client: Optional[AsyncKernelClient] = None

    child_env: MutableMapping[str, str]
    subproc: Optional[asyncio.subprocess.Process]
    service_parser: Optional[ServiceParser]
    runtime_path: Path
    services_running: Dict[str, asyncio.subprocess.Process]
    intrinsic_host_ports_mapping: Mapping[str, int]
    _build_success: Optional[bool]
    user_input_queue: Optional[asyncio.Queue[str]]
    _main_task: asyncio.Task
    _run_task: asyncio.Task
    _health_check_task: Optional[asyncio.Task]

    def __init__(self, runtime_path: Path) -> None:
        self.subproc = None
        self.runtime_path = runtime_path
        self.child_env = {**os.environ, **self.default_child_env}
        if "PATH" not in self.child_env:
            self.child_env["PATH"] = self.default_child_env_path
        if "SHELL" not in self.child_env:
            self.child_env["SHELL"] = self.default_child_env_shell
        config_dir = Path("/home/config")
        try:
            evdata = (config_dir / "environ.txt").read_text()
            for line in evdata.splitlines():
                k, v = line.split("=", 1)
                self.child_env[k] = v
                os.environ[k] = v
        except FileNotFoundError:
            pass
        except Exception:
            log.exception("Reading /home/config/environ.txt failed!")

        path_env = self.child_env["PATH"]
        if Path("/usr/local/cuda/bin").is_dir():
            path_env = promote_path(path_env, "/usr/local/cuda/bin")
        if Path("/usr/local/nvidia/bin").is_dir():
            path_env = promote_path(path_env, "/usr/local/nvidia/bin")
        if Path("/home/linuxbrew/.linuxbrew").is_dir():
            path_env = promote_path(path_env, "/home/linuxbrew/.linuxbrew/bin")
        path_env = promote_path(path_env, "/home/work/.local/bin")
        self.child_env["PATH"] = path_env

        self.started_at: float = time.monotonic()
        self.services_running = {}

        self.intrinsic_host_ports_mapping = {}

        self.user_input_queue = None

        self._build_success = None

        self._health_check_task = None

    async def _init(self, cmdargs) -> None:
        self.cmdargs = cmdargs
        loop = current_loop()
        self._service_lock = asyncio.Lock()

        executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)
        loop.set_default_executor(executor)

        self.zctx = zmq.asyncio.Context()

        intrinsic_host_ports_mapping_path = Path("/home/config/intrinsic-ports.json")
        if intrinsic_host_ports_mapping_path.is_file():
            intrinsic_host_ports_mapping = json.loads(
                await asyncio.get_running_loop().run_in_executor(
                    None,
                    lambda: intrinsic_host_ports_mapping_path.read_text(),
                )
            )
            self.intrinsic_host_ports_mapping = intrinsic_host_ports_mapping

    async def _shutdown(self) -> None:
        try:
            self.insock.close()
            log.debug("shutting down...")
            self._run_task.cancel()
            self._main_task.cancel()
            await self._run_task
            await self._main_task
            if health_check_task := self._health_check_task:
                health_check_task.cancel()
                await health_check_task
            log.debug("terminating service processes...")
            running_procs = [*self.services_running.values()]
            async with self._service_lock:
                await asyncio.gather(
                    *(terminate_and_wait(proc) for proc in running_procs),
                    return_exceptions=True,
                )
                await asyncio.sleep(0.01)
            log.debug("terminated.")
        finally:
            await asyncio.sleep(0.1)
            try:
                if self.outsock:
                    self.outsock.close()
                await self._shutdown_jupyter_kernel()
            finally:
                self._log_task.cancel()
                await self._log_task

    async def _init_jupyter_kernel(self) -> None:

        kconfigdir = Path("/home/work/.ipython/profile_default/")
        kconfigdir.mkdir(parents=True, exist_ok=True)
        kconfig_file = kconfigdir / "ipython_kernel_config.py"
        kconfig_file.write_text("c.InteractiveShellApp.matplotlib = 'inline'")

        kernelspec_mgr = KernelSpecManager()
        kernelspec_mgr.ensure_native_kernel = False
        kspecs = kernelspec_mgr.get_all_specs()
        for kname in kspecs:
            if self.jupyter_kspec_name in kname:
                log.debug("starting " + kname + " kernel...")
                self.kernel_mgr = AsyncKernelManager(kernel_name=kname)
                await self.kernel_mgr.start_kernel()
                if not await self.kernel_mgr.is_alive():
                    log.error("jupyter query mode is disabled: failed to start jupyter kernel")
                else:
                    self.kernel_client = self.kernel_mgr.client() 
                    assert self.kernel_client is not None
                    self.kernel_client.start_channels(shell=True, iopub=True, stdin=True, hb=True)
                    try:
                        await self.kernel_client.wait_for_ready(timeout=10)
                    except RuntimeError:
                        log.error("jupyter channel is not active!")
                        self.kernel_mgr = None
                break
        else:
            log.debug("jupyter query mode is not available: no jupyter kernelspec found")
            self.kernel_mgr = None

    async def _shutdown_jupyter_kernel(self):
        if self.kernel_mgr and await self.kernel_mgr.is_alive():
            assert self.kernel_client is not None
            log.info("shutting down " + self.jupyter_kspec_name + " kernel...")
            await self.kernel_mgr.shutdown_kernel()
            self.kernel_client.stop_channels()
            assert not await self.kernel_mgr.is_alive(), "ipykernel failed to shutdown"

    async def _handle_exception(
        self, coro: Awaitable[TReturn], help_text: str | None = None
    ) -> TReturn | FailureSentinel:
        try:
            return await coro
        except Exception as e:
            match e:
                case FileNotFoundError():
                    msg = "File not found: {!r}"
                    if help_text:
                        msg += f" ({help_text})"
                    log.exception(msg, e.filename)
                case _:
                    msg = "Unexpected error!"
                    if help_text:
                        msg += f" ({help_text})"
                    log.exception(msg)
            return FAILURE

    async def _init_with_loop(self) -> None:
        if self.init_done is not None:
            self.init_done.clear()
        try:
            ret = await self._handle_exception(
                self.init_with_loop(),
                "Check the image configs/labels like `ai.backend.runtime-path`",
            )
            if ret is FAILURE:
                log.warning(
                    "We are skipping the runtime-specific initialization failure, "
                    "and the container may not work as expected."
                )
            await self._handle_exception(
                init_sshd_service(self.child_env),
                "Verify agent installation with the embedded prebuilt binaries",
            )
        finally:
            if self.init_done is not None:
                self.init_done.set()

    @abstractmethod
    async def init_with_loop(self) -> None:

    async def _clean(self, clean_cmd: Optional[str]) -> None:
        ret = 0
        try:
            if clean_cmd is None or clean_cmd == "":
                return
            elif clean_cmd == "*":
                ret = await self.clean_heuristic()
            else:
                ret = await self.run_subproc(clean_cmd)
        except Exception:
            log.exception("unexpected error")
            ret = -1
        finally:
            await asyncio.sleep(0.01)
            payload = json.dumps({
                "exitCode": ret,
            }).encode("utf8")
            await self.outsock.send_multipart([b"clean-finished", payload])

    async def clean_heuristic(self) -> int:
        return 0

    async def _bootstrap(self, script_path: Path) -> None:
        log.info("Running the user bootstrap script...")
        ret = 0
        try:
            ret = await self.run_subproc(["/bin/sh", str(script_path)])
        except Exception:
            log.exception("unexpected error while executing the user bootstrap script")
            ret = -1
        finally:
            await asyncio.sleep(0.01)
            log.info("The user bootstrap script has exited with code {}", ret)

    async def _build(self, build_cmd: Optional[str]) -> None:
        ret = 0
        try:
            if build_cmd is None or build_cmd == "":
                return
            elif build_cmd == "*":
                if Path("Makefile").is_file():
                    ret = await self.run_subproc("make")
                else:
                    ret = await self.build_heuristic()
            else:
                ret = await self.run_subproc(build_cmd)
        except Exception:
            log.exception("unexpected error")
            ret = -1
        finally:
            await asyncio.sleep(0.01)
            self._build_success = ret == 0
            payload = json.dumps({
                "exitCode": ret,
            }).encode("utf8")
            await self.outsock.send_multipart([b"build-finished", payload])

    @abstractmethod
    async def build_heuristic(self) -> int:
        """Process build step."""

    async def _execute(self, exec_cmd: str) -> None:
        ret = 0
        try:
            if exec_cmd is None or exec_cmd == "":
                return
            elif exec_cmd == "*":
                ret = await self.execute_heuristic()
            else:
                ret = await self.run_subproc(exec_cmd, batch=True)
        except Exception:
            log.exception("unexpected error")
            ret = -1
        finally:
            await asyncio.sleep(0.01)
            payload = json.dumps({
                "exitCode": ret,
            }).encode("utf8")
            await self.outsock.send_multipart([b"finished", payload])

    @abstractmethod
    async def execute_heuristic(self) -> int:

    async def _query(self, code_text: str) -> None:
        ret = 0
        try:
            ret = await self.query(code_text)
        except Exception:
            log.exception("unexpected error")
            ret = -1
        finally:
            payload = json.dumps({
                "exitCode": ret,
            }).encode("utf8")
            await self.outsock.send_multipart([b"finished", payload])

    async def query(self, code_text) -> int:
        if not hasattr(self, "kernel_mgr") or self.kernel_mgr is None:
            log.error("query mode is disabled: failed to start jupyter kernel")
            return 127

        assert self.kernel_client is not None
        log.debug("executing in query mode...")