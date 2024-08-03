from __future__ import annotations

import abc
import asyncio
import enum
import hashlib
import logging
import secrets
import socket
import uuid
from collections import defaultdict
from typing import (
    Any,
    Callable,
    ClassVar,
    Coroutine,
    Generic,
    Mapping,
    Optional,
    Protocol,
    Type,
    TypedDict,
    TypeVar,
    Union,
    cast,
)

import attrs
from aiomonitor.task import preserve_termination_log
from aiotools.context import aclosing
from aiotools.server import process_index
from aiotools.taskgroup import PersistentTaskGroup
from aiotools.taskgroup.types import AsyncExceptionHandler
from redis.asyncio import ConnectionPool

from . import msgpack, redis_helper
from .logging import BraceStyleAdapter
from .types import (
    AgentId,
    EtcdRedisConfig,
    KernelId,
    LogSeverity,
    ModelServiceStatus,
    QuotaScopeID,
    RedisConnectionInfo,
    SessionId,
    VolumeMountableNodeType,
    aobject,
)

__all__ = (
    "AbstractEvent",
    "EventCallback",
    "EventDispatcher",
    "EventHandler",
    "EventProducer",
)

log = BraceStyleAdapter(logging.getLogger(__spec__.name))  # type: ignore[name-defined]


class AbstractEvent(metaclass=abc.ABCMeta):
    name: ClassVar[str] = "undefined"

    @abc.abstractmethod
    def serialize(self) -> tuple:
        pass

    @classmethod
    @abc.abstractmethod
    def deserialize(cls, value: tuple):
        pass


class EmptyEventArgs:
    def serialize(self) -> tuple:
        return tuple()

    @classmethod
    def deserialize(cls, value: tuple):
        return cls()


class DoScheduleEvent(EmptyEventArgs, AbstractEvent):
    name = "do_schedule"


class DoPrepareEvent(EmptyEventArgs, AbstractEvent):
    name = "do_prepare"


class DoScaleEvent(EmptyEventArgs, AbstractEvent):
    name = "do_scale"


class DoIdleCheckEvent(EmptyEventArgs, AbstractEvent):
    name = "do_idle_check"


class DoUpdateSessionStatusEvent(EmptyEventArgs, AbstractEvent):
    name = "do_update_session_status"


@attrs.define(slots=True, frozen=True)
class DoTerminateSessionEvent(AbstractEvent):
    name = "do_terminate_session"

    session_id: SessionId = attrs.field()
    reason: KernelLifecycleEventReason = attrs.field()

    def serialize(self) -> tuple:
        return (
            str(self.session_id),
            self.reason,
        )

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(
            SessionId(uuid.UUID(value[0])),
            value[1],
        )


@attrs.define(slots=True, frozen=True)
class GenericAgentEventArgs:
    reason: str = attrs.field(default="")

    def serialize(self) -> tuple:
        return (self.reason,)

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(value[0])


class AgentStartedEvent(GenericAgentEventArgs, AbstractEvent):
    name = "agent_started"


class AgentTerminatedEvent(GenericAgentEventArgs, AbstractEvent):
    name = "agent_terminated"


@attrs.define(slots=True, frozen=True)
class AgentErrorEvent(AbstractEvent):
    name = "agent_error"

    message: str = attrs.field()
    traceback: Optional[str] = attrs.field(default=None)
    user: Optional[Any] = attrs.field(default=None)
    context_env: Mapping[str, Any] = attrs.field(factory=dict)
    severity: LogSeverity = attrs.field(default=LogSeverity.ERROR)

    def serialize(self) -> tuple:
        return (
            self.message,
            self.traceback,
            self.user,
            self.context_env,
            self.severity.value,
        )

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(
            value[0],
            value[1],
            value[2],
            value[3],
            LogSeverity(value[4]),
        )