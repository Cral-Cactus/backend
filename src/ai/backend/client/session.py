from __future__ import annotations

import abc
import asyncio
import inspect
import queue
import threading
import warnings
from contextvars import Context, ContextVar, copy_context
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Coroutine,
    Iterator,
    Literal,
    Tuple,
    TypeVar,
    Union,
)

import aiohttp
from multidict import CIMultiDict

from .config import MIN_API_VERSION, APIConfig, get_config, parse_api_version
from .exceptions import APIVersionWarning, BackendAPIError, BackendClientError
from .types import Sentinel, sentinel

__all__ = (
    "BaseSession",
    "Session",
    "AsyncSession",
    "api_session",
)

from ..common.types import SSLContextType

api_session: ContextVar[BaseSession] = ContextVar("api_session")


async def _negotiate_api_version(
    http_session: aiohttp.ClientSession,
    config: APIConfig,
) -> Tuple[int, str]:
    client_version = parse_api_version(config.version)
    try:
        timeout_config = aiohttp.ClientTimeout(
            total=None,
            connect=None,
            sock_connect=config.connection_timeout,
            sock_read=config.read_timeout,
        )
        headers = CIMultiDict([
            ("User-Agent", config.user_agent),
        ])
        probe_url = (
            config.endpoint / "func/" if config.endpoint_type == "session" else config.endpoint
        )
        async with http_session.get(probe_url, timeout=timeout_config, headers=headers) as resp:
            resp.raise_for_status()
            server_info = await resp.json()
            server_version = parse_api_version(server_info["version"])
            if server_version > client_version:
                warnings.warn(
                    "The server API version is higher than the client. "
                    category=APIVersionWarning,
                )
            if server_version < MIN_API_VERSION:
                warnings.warn(
                    f"The server is too old ({server_version}) and does not meet the minimum API version"
                    f" requirement: v{MIN_API_VERSION[0]}.{MIN_API_VERSION[1]}\nPlease upgrade"
                    category=APIVersionWarning,
                )
            return min(server_version, client_version)
    except (asyncio.TimeoutError, aiohttp.ClientError):
        return client_version


async def _close_aiohttp_session(session: aiohttp.ClientSession) -> None:
    transports = 0
    all_is_lost = asyncio.Event()
    if session.connector is None:
        all_is_lost.set()
    else:
        if len(session.connector._conns) == 0:
            all_is_lost.set()
        for conn in session.connector._conns.values():
            for handler, _ in conn:
                proto = getattr(handler.transport, "_ssl_protocol", None)
                if proto is None:
                    continue
                transports += 1
                orig_lost = proto.connection_lost
                orig_eof_received = proto.eof_received

                def connection_lost(exc):
                    orig_lost(exc)
                    nonlocal transports
                    transports -= 1
                    if transports == 0:
                        all_is_lost.set()

                def eof_received():
                    try:
                        orig_eof_received()
                    except AttributeError:
                        pass

                proto.connection_lost = connection_lost
                proto.eof_received = eof_received
    await session.close()
    if transports > 0:
        await all_is_lost.wait()


_Item = TypeVar("_Item")


class _SyncWorkerThread(threading.Thread):
    work_queue: queue.Queue[
        Union[
            Tuple[Union[AsyncIterator, Coroutine], Context],
            Sentinel,
        ]
    ]
    done_queue: queue.Queue[Union[Any, Exception]]
    stream_queue: queue.Queue[Union[Any, Exception, Sentinel]]
    stream_block: threading.Event
    agen_shutdown: bool

    __slots__ = (
        "work_queue",
        "done_queue",
        "stream_queue",
        "stream_block",
        "agen_shutdown",
    )

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.work_queue = queue.Queue()
        self.done_queue = queue.Queue()
        self.stream_queue = queue.Queue()
        self.stream_block = threading.Event()
        self.agen_shutdown = False

    def run(self) -> None:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            while True:
                item = self.work_queue.get()
                if item is sentinel:
                    break
                coro, ctx = item
                if inspect.isasyncgen(coro):
                    ctx.run(loop.run_until_complete, self.agen_wrapper(coro))
                else:
                    try:
                        result = ctx.run(loop.run_until_complete, coro)  # type: ignore
                    except Exception as e:
                        self.done_queue.put_nowait(e)
                    else:
                        self.done_queue.put_nowait(result)
                self.work_queue.task_done()
        except (SystemExit, KeyboardInterrupt):
            pass
        finally:
            loop.run_until_complete(loop.shutdown_asyncgens())
            loop.stop()
            loop.close()

    def execute(self, coro: Coroutine) -> Any:
        ctx = copy_context()  # preserve context for the worker thread
        try:
            self.work_queue.put((coro, ctx))
            result = self.done_queue.get()
            self.done_queue.task_done()
            if isinstance(result, Exception):
                raise result
            return result
        finally:
            del ctx

    async def agen_wrapper(self, agen):
        self.agen_shutdown = False
        try:
            async for item in agen:
                self.stream_block.clear()
                self.stream_queue.put(item)
                # flow-control the generator.
                self.stream_block.wait()
                if self.agen_shutdown:
                    break
        except Exception as e:
            self.stream_queue.put(e)
        finally:
            self.stream_queue.put(sentinel)
            await agen.aclose()

    def execute_generator(self, asyncgen: AsyncIterator[_Item]) -> Iterator[_Item]:
        ctx = copy_context()  # preserve context for the worker thread
        try:
            self.work_queue.put((asyncgen, ctx))
            while True:
                item = self.stream_queue.get()
                try:
                    if item is sentinel:
                        break
                    if isinstance(item, Exception):
                        raise item
                    yield item
                finally:
                    self.stream_block.set()
                    self.stream_queue.task_done()
        finally:
            del ctx

    def interrupt_generator(self):
        self.agen_shutdown = True
        self.stream_block.set()
        self.stream_queue.put(sentinel)


class BaseSession(metaclass=abc.ABCMeta):
    """
    The base abstract class for sessions.
    """

    __slots__ = (
        "_config",
        "_closed",
        "_context_token",
        "_proxy_mode",
        "aiohttp_session",
        "api_version",
        "System",
        "Manager",
        "Admin",
        "Agent",
        "AgentWatcher",
        "ScalingGroup",
        "Storage",
        "Image",
        "ComputeSession",
        "SessionTemplate",
        "Domain",
        "Group",
        "Auth",
        "User",
        "KeyPair",
        "BackgroundTask",
        "EtcdConfig",
        "Resource",
        "KeypairResourcePolicy",
        "VFolder",
        "Dotfile",
        "ServerLog",
        "Permission",
        "Service",
        "Model",
        "QuotaScope",
    )