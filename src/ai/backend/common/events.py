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
class AgentHeartbeatEvent(AbstractEvent):
    name = "agent_heartbeat"

    agent_info: Mapping[str, Any] = attrs.field()

    def serialize(self) -> tuple:
        return (self.agent_info,)

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(value[0])


@attrs.define(slots=True, frozen=True)
class DoAgentResourceCheckEvent(AbstractEvent):
    name = "do_agent_resource_check"

    agent_id: AgentId = attrs.field()

    def serialize(self) -> tuple:
        return (self.agent_id,)

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(
            AgentId(value[0]),
        )


class KernelLifecycleEventReason(enum.StrEnum):
    AGENT_TERMINATION = "agent-termination"
    ALREADY_TERMINATED = "already-terminated"
    ANOMALY_DETECTED = "anomaly-detected"
    EXEC_TIMEOUT = "exec-timeout"
    FAILED_TO_CREATE = "failed-to-create"
    FAILED_TO_START = "failed-to-start"
    FORCE_TERMINATED = "force-terminated"
    HANG_TIMEOUT = "hang-timeout"
    IDLE_TIMEOUT = "idle-timeout"
    IDLE_SESSION_LIFETIME = "idle-session-lifetime"
    IDLE_UTILIZATION = "idle-utilization"
    KILLED_BY_EVENT = "killed-by-event"
    SERVICE_SCALED_DOWN = "service-scaled-down"
    NEW_CONTAINER_STARTED = "new-container-started"
    PENDING_TIMEOUT = "pending-timeout"
    RESTARTING = "restarting"
    RESTART_TIMEOUT = "restart-timeout"
    RESUMING_AGENT_OPERATION = "resuming-agent-operation"
    SELF_TERMINATED = "self-terminated"
    TASK_DONE = "task-done"
    TASK_FAILED = "task-failed"
    TASK_TIMEOUT = "task-timeout"
    TASK_CANCELLED = "task-cancelled"
    TASK_FINISHED = "task-finished"
    TERMINATED_UNKNOWN_CONTAINER = "terminated-unknown-container"
    UNKNOWN = "unknown"
    USER_REQUESTED = "user-requested"
    NOT_FOUND_IN_MANAGER = "not-found-in-manager"
    CONTAINER_NOT_FOUND = "container-not-found"

    @classmethod
    def from_value(cls, value: Optional[str]) -> Optional[KernelLifecycleEventReason]:
        if value is None:
            return None
        try:
            return cls(value)
        except ValueError:
            pass
        return None


@attrs.define(slots=True, frozen=True)
class KernelCreationEventArgs:
    kernel_id: KernelId = attrs.field()
    session_id: SessionId = attrs.field()
    reason: str = attrs.field(default="")
    creation_info: Mapping[str, Any] = attrs.field(factory=dict)

    def serialize(self) -> tuple:
        return (
            str(self.kernel_id),
            str(self.session_id),
            self.reason,
            self.creation_info,
        )

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(
            kernel_id=KernelId(uuid.UUID(value[0])),
            session_id=SessionId(uuid.UUID(value[1])),
            reason=value[2],
            creation_info=value[3],
        )


class KernelPreparingEvent(KernelCreationEventArgs, AbstractEvent):
    name = "kernel_preparing"


class KernelPullingEvent(KernelCreationEventArgs, AbstractEvent):
    name = "kernel_pulling"


@attrs.define(auto_attribs=True, slots=True)
class KernelPullProgressEvent(AbstractEvent):
    name = "kernel_pull_progress"
    kernel_id: uuid.UUID = attrs.field()
    current_progress: float = attrs.field()
    total_progress: float = attrs.field()
    message: Optional[str] = attrs.field(default=None)

    def serialize(self) -> tuple:
        return (
            str(self.kernel_id),
            self.current_progress,
            self.total_progress,
            self.message,
        )

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(
            uuid.UUID(value[0]),
            value[1],
            value[2],
            value[3],
        )


class KernelCreatingEvent(KernelCreationEventArgs, AbstractEvent):
    name = "kernel_creating"


class KernelStartedEvent(KernelCreationEventArgs, AbstractEvent):
    name = "kernel_started"


class KernelCancelledEvent(KernelCreationEventArgs, AbstractEvent):
    name = "kernel_cancelled"


@attrs.define(slots=True, frozen=True)
class ModelServiceStatusEventArgs:
    kernel_id: KernelId = attrs.field()
    session_id: SessionId = attrs.field()
    model_name: str = attrs.field()
    new_status: ModelServiceStatus = attrs.field()

    def serialize(self) -> tuple:
        return (
            str(self.kernel_id),
            str(self.session_id),
            self.model_name,
            self.new_status.value,
        )

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(
            kernel_id=KernelId(uuid.UUID(value[0])),
            session_id=SessionId(uuid.UUID(value[1])),
            model_name=value[2],
            new_status=ModelServiceStatus(value[3]),
        )


class ModelServiceStatusEvent(ModelServiceStatusEventArgs, AbstractEvent):
    name = "model_service_status_updated"


@attrs.define(slots=True, frozen=True)
class KernelTerminationEventArgs:
    kernel_id: KernelId = attrs.field()
    session_id: SessionId = attrs.field()
    reason: KernelLifecycleEventReason = attrs.field(default=KernelLifecycleEventReason.UNKNOWN)
    exit_code: int = attrs.field(default=-1)

    def serialize(self) -> tuple:
        return (
            str(self.kernel_id),
            str(self.session_id),
            self.reason,
            self.exit_code,
        )

        @classmethod
    def deserialize(cls, value: tuple):
        return cls(
            SessionId(uuid.UUID(value[0])),
            value[1],
            value[2],
        )


class SessionEnqueuedEvent(SessionCreationEventArgs, AbstractEvent):
    name = "session_enqueued"


class SessionScheduledEvent(SessionCreationEventArgs, AbstractEvent):
    name = "session_scheduled"


class SessionPreparingEvent(SessionCreationEventArgs, AbstractEvent):
    name = "session_preparing"


class SessionCancelledEvent(SessionCreationEventArgs, AbstractEvent):
    name = "session_cancelled"


class SessionStartedEvent(SessionCreationEventArgs, AbstractEvent):
    name = "session_started"


@attrs.define(slots=True, frozen=True)
class SessionTerminationEventArgs:
    session_id: SessionId = attrs.field()
    reason: str = attrs.field(default="")

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


class SessionTerminatingEvent(SessionTerminationEventArgs, AbstractEvent):
    name = "session_terminating"


class SessionTerminatedEvent(SessionTerminationEventArgs, AbstractEvent):
    name = "session_terminated"


@attrs.define(slots=True, frozen=True)
class SessionResultEventArgs:
    session_id: SessionId = attrs.field()
    reason: KernelLifecycleEventReason = attrs.field(default=KernelLifecycleEventReason.UNKNOWN)
    exit_code: int = attrs.field(default=-1)

    def serialize(self) -> tuple:
        return (
            str(self.session_id),
            self.reason,
            self.exit_code,
        )

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(
            SessionId(uuid.UUID(value[0])),
            value[1],
            value[2],
        )


class SessionSuccessEvent(SessionResultEventArgs, AbstractEvent):
    name = "session_success"


class SessionFailureEvent(SessionResultEventArgs, AbstractEvent):
    name = "session_failure"


@attrs.define(slots=True, frozen=True)
class RouteCreationEventArgs:
    route_id: uuid.UUID = attrs.field()

    def serialize(self) -> tuple:
        return (str(self.route_id),)

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(uuid.UUID(value[0]))


class RouteCreatedEvent(RouteCreationEventArgs, AbstractEvent):
    name = "route_created"


@attrs.define(auto_attribs=True, slots=True)
class DoSyncKernelLogsEvent(AbstractEvent):
    name = "do_sync_kernel_logs"

    kernel_id: KernelId = attrs.field()
    container_id: str = attrs.field()

    def serialize(self) -> tuple:
        return (
            str(self.kernel_id),
            self.container_id,
        )

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(
            KernelId(uuid.UUID(value[0])),
            value[1],
        )


@attrs.define(auto_attribs=True, slots=True)
class GenericSessionEventArgs(AbstractEvent):
    session_id: SessionId = attrs.field()

    def serialize(self) -> tuple:
        return (str(self.session_id),)

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(
            SessionId(uuid.UUID(value[0])),
        )


class ExecutionStartedEvent(GenericSessionEventArgs, AbstractEvent):
    name = "execution_started"


class ExecutionFinishedEvent(GenericSessionEventArgs, AbstractEvent):
    name = "execution_finished"


class ExecutionTimeoutEvent(GenericSessionEventArgs, AbstractEvent):
    name = "execution_timeout"


class ExecutionCancelledEvent(GenericSessionEventArgs, AbstractEvent):
    name = "execution_cancelled"


@attrs.define(auto_attribs=True, slots=True)
class BgtaskUpdatedEvent(AbstractEvent):
    name = "bgtask_updated"

    task_id: uuid.UUID = attrs.field()
    current_progress: float = attrs.field()
    total_progress: float = attrs.field()
    message: Optional[str] = attrs.field(default=None)

    def serialize(self) -> tuple:
        return (
            str(self.task_id),
            self.current_progress,
            self.total_progress,
            self.message,
        )

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(
            uuid.UUID(value[0]),
            value[1],
            value[2],
            value[3],
        )


@attrs.define(auto_attribs=True, slots=True)
class BgtaskDoneEventArgs:
    task_id: uuid.UUID = attrs.field()
    message: Optional[str] = attrs.field(default=None)

    def serialize(self) -> tuple:
        return (
            str(self.task_id),
            self.message,
        )

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(
            uuid.UUID(value[0]),
            value[1],
        )


class BgtaskDoneEvent(BgtaskDoneEventArgs, AbstractEvent):
    name = "bgtask_done"


class BgtaskCancelledEvent(BgtaskDoneEventArgs, AbstractEvent):
    name = "bgtask_cancelled"


class BgtaskFailedEvent(BgtaskDoneEventArgs, AbstractEvent):
    name = "bgtask_failed"


@attrs.define(slots=True)
class DoVolumeMountEvent(AbstractEvent):
    name = "do_volume_mount"

    dir_name: str = attrs.field()
    volume_backend_name: str = attrs.field()
    quota_scope_id: QuotaScopeID = attrs.field()

    fs_location: str = attrs.field()
    fs_type: str = attrs.field(default="nfs")
    cmd_options: str | None = attrs.field(default=None)
    scaling_group: str | None = attrs.field(default=None)

    edit_fstab: bool = attrs.field(default=False)
    fstab_path: str = attrs.field(default="/etc/fstab")

    def serialize(self) -> tuple:
        return (
            self.dir_name,
            self.volume_backend_name,
            str(self.quota_scope_id),
            self.fs_location,
            self.fs_type,
            self.cmd_options,
            self.scaling_group,
            self.edit_fstab,
            self.fstab_path,
        )

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(
            dir_name=value[0],
            volume_backend_name=value[1],
            quota_scope_id=QuotaScopeID.parse(value[2]),
            fs_location=value[3],
            fs_type=value[4],
            cmd_options=value[5],
            scaling_group=value[6],
            edit_fstab=value[7],
            fstab_path=value[8],
        )