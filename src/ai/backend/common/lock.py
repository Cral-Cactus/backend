from __future__ import annotations

import abc
import asyncio
import fcntl
import logging
from collections.abc import Mapping
from io import IOBase
from pathlib import Path
from typing import Any, ClassVar, Optional

import trafaret as t
from etcd_client import Client as EtcdClient
from etcd_client import Communicator as EtcdCommunicator
from etcd_client import EtcdLockOption
from etcetra.client import EtcdCommunicator as EtcetraCommunicator
from etcetra.client import EtcdConnectionManager as EtcetraConnectionManager
from redis.asyncio import Redis
from redis.asyncio.lock import Lock as AsyncRedisLock
from redis.exceptions import LockError, LockNotOwnedError
from tenacity import (
    AsyncRetrying,
    RetryError,
    retry_if_exception_type,
    stop_after_delay,
    stop_never,
    wait_exponential,
    wait_random,
)

from ai.backend.common.etcd import AsyncEtcd
from ai.backend.common.etcd_etcetra import AsyncEtcd as EtcetraAsyncEtcd
from ai.backend.common.types import RedisConnectionInfo

from .logging import BraceStyleAdapter

log = BraceStyleAdapter(logging.getLogger(__spec__.name))


class AbstractDistributedLock(metaclass=abc.ABCMeta):
    default_config: ClassVar[Mapping[str, Any]] = {}
    config_iv: ClassVar[t.Trafaret] = t.Dict().allow_extra("*")

    def __init__(self, *, lifetime: Optional[float] = None) -> None:
        assert lifetime is None or lifetime >= 0.0
        self._lifetime = lifetime

    @abc.abstractmethod
    async def __aenter__(self) -> Any:
        raise NotImplementedError

    @abc.abstractmethod
    async def __aexit__(self, *exc_info) -> Optional[bool]:
        raise NotImplementedError


class FileLock(AbstractDistributedLock):
    default_timeout: float = 3

    _file: IOBase | None
    _locked: bool = False

    def __init__(
        self,
        path: Path,
        *,
        timeout: Optional[float] = None,
        lifetime: Optional[float] = None,
        remove_when_unlock: bool = False,
        debug: bool = False,
    ) -> None:
        super().__init__(lifetime=lifetime)
        self._file = None
        self._path = path
        self._timeout = timeout if timeout is not None else self.default_timeout
        self._debug = debug
        self._remove_when_unlock = remove_when_unlock
        self._watchdog_task: Optional[asyncio.Task[Any]] = None