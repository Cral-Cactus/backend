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

        insock_port = self.intrinsic_host_ports_mapping.get("replin", "2000")
        outsock_port = self.intrinsic_host_ports_mapping.get("replout", "2001")
        self.insock = self.zctx.socket(zmq.PULL)
        self.insock.bind(f"tcp://*:{insock_port}")
        print(f"binding to tcp://*:{insock_port}")
        self.outsock = self.zctx.socket(zmq.PUSH)
        self.outsock.bind(f"tcp://*:{outsock_port}")
        print(f"binding to tcp://*:{outsock_port}")

        self.log_queue = janus.Queue()
        self.task_queue = asyncio.Queue()
        self.init_done = asyncio.Event()

        setup_logger(self.log_queue.sync_q, self.log_prefix, cmdargs.debug)
        self._log_task = loop.create_task(self._handle_logs())
        await asyncio.sleep(0)

        self.service_parser = ServiceParser({
            "runtime_path": str(self.runtime_path),
        })
        service_def_folder = Path("/etc/backend/service-defs")
        if service_def_folder.is_dir():
            await self.service_parser.parse(service_def_folder)
            log.debug("Loaded new-style service definitions.")

        self._main_task = loop.create_task(self.main_loop(cmdargs))
        self._run_task = loop.create_task(self.run_tasks())