from __future__ import annotations

import asyncio
import enum
import logging
import math
from abc import ABCMeta, abstractmethod
from collections import UserDict, defaultdict
from datetime import datetime, timedelta
from decimal import Decimal
import aiotools
import sqlalchemy as sa
import trafaret as t
from aiotools import TaskGroupError
from sqlalchemy.engine import Row

import ai.backend.common.validators as tx
from ai.backend.common import msgpack, redis_helper
from ai.backend.common.logging import BraceStyleAdapter
from ai.backend.common.types import (
    AccessKey,
    BinarySize,
    RedisConnectionInfo,
    SessionTypes,
)
from ai.backend.common.utils import nmget

if TYPE_CHECKING:
    from sqlalchemy.ext.asyncio import AsyncConnection as SAConnection

    from ai.backend.common.types import AgentId, KernelId, SessionId

    from .config import SharedConfig
    from .models.utils import ExtendedAsyncSAEngine as SAEngine

log = BraceStyleAdapter(logging.getLogger(__spec__.name))

DEFAULT_CHECK_INTERVAL: Final = 15.0
IDLE_TIMEOUT_VALUE: Final = -1


class IdleCheckerError(TaskGroupError):

def parse_unit(resource_name: str, value: float | int) -> float | int:
    if resource_name.find("mem") == -1:
        return value
    return BinarySize(int(value))


def calculate_remaining_time(
    now: datetime,
    idle_baseline: datetime,
    timeout_period: timedelta,
    grace_period_end: Optional[datetime] = None,
) -> float:
    if grace_period_end is None:
        baseline = idle_baseline
    else:
        baseline = max(idle_baseline, grace_period_end)
    remaining = baseline - now + timeout_period
    return remaining.total_seconds()


async def get_redis_now(redis_obj: RedisConnectionInfo) -> float:
    t = await redis_helper.execute(redis_obj, lambda r: r.time())
    return t[0] + (t[1] / (10**6))


async def get_db_now(dbconn: SAConnection) -> datetime:
    return await dbconn.scalar(sa.select(sa.func.now()))


class UtilizationExtraInfo(NamedTuple):
    avg_util: float
    threshold: float

    def to_dict(self, apply_unit: bool = True) -> dict[str, UtilizationExtraInfo]:
        if apply_unit:
            return {
                k: UtilizationExtraInfo(parse_unit(k, v[0]), parse_unit(k, v[1]))
                for k, v in self.data.items()
            }
        return {**self.data}

    @property
    def utilization_result(self) -> dict[str, bool]:
        return {k: v.avg_util >= v.threshold for k, v in self.data.items()}


class AppStreamingStatus(enum.Enum):
    NO_ACTIVE_CONNECTIONS = 0
    HAS_ACTIVE_CONNECTIONS = 1


class ThresholdOperator(enum.Enum):
    AND = "and"
    OR = "or"


class RemainingTimeType(enum.StrEnum):
    GRACE_PERIOD = "grace_period"
    EXPIRE_AFTER = "expire_after"


class IdleCheckerHost:
    check_interval: ClassVar[float] = DEFAULT_CHECK_INTERVAL

    def __init__(
        self,
        db: SAEngine,
        shared_config: SharedConfig,
        event_dispatcher: EventDispatcher,
        event_producer: EventProducer,
        lock_factory: DistributedLockFactory,
    ) -> None:
        self._checkers: list[BaseIdleChecker] = []
        self._frozen = False
        self._db = db
        self._shared_config = shared_config
        self._event_dispatcher = event_dispatcher
        self._event_producer = event_producer
        self._lock_factory = lock_factory
        self._redis_live = redis_helper.get_redis_object(
            self._shared_config.data["redis"],
            name="idle.live",
            db=REDIS_LIVE_DB,
        )
        self._redis_stat = redis_helper.get_redis_object(
            self._shared_config.data["redis"],
            name="idle.stat",
            db=REDIS_STAT_DB,
        )
        self._grace_period_checker: NewUserGracePeriodChecker = NewUserGracePeriodChecker(
            event_dispatcher, self._redis_live, self._redis_stat
        )

    def add_checker(self, checker: BaseIdleChecker):
        if self._frozen:
            raise RuntimeError(
                "Cannot add a new idle checker after the idle checker host is frozen."
            )
        self._checkers.append(checker)

    async def start(self) -> None:
        self._frozen = True
        raw_config = await self._shared_config.etcd.get_prefix_dict(
            "config/idle/checkers",
        )
        raw_config = cast(Mapping[str, Mapping[str, Any]], raw_config)
        await self._grace_period_checker.populate_config(
            raw_config.get(self._grace_period_checker.name) or {}
        )
        for checker in self._checkers:
            await checker.populate_config(raw_config.get(checker.name) or {})
        self.timer = GlobalTimer(
            self._lock_factory(LockID.LOCKID_IDLE_CHECK_TIMER, self.check_interval),
            self._event_producer,
            lambda: DoIdleCheckEvent(),
            self.check_interval,
            task_name="idle_checker",
        )
        self._evh_idle_check = self._event_dispatcher.consume(
            DoIdleCheckEvent,
            None,
            self._do_idle_check,
        )
        await self.timer.join()

    async def shutdown(self) -> None:
        for checker in self._checkers:
            await checker.aclose()
        await self.timer.leave()
        self._event_dispatcher.unconsume(self._evh_idle_check)
        await self._redis_stat.close()
        await self._redis_live.close()

    async def update_app_streaming_status(
        self,
        session_id: SessionId,
        status: AppStreamingStatus,
    ) -> None:
        for checker in self._checkers:
            await checker.update_app_streaming_status(session_id, status)

    async def _do_idle_check(
        self,
        context: None,
        source: AgentId,
        event: DoIdleCheckEvent,
    ) -> None:
        log.debug("do_idle_check(): triggered")
        policy_cache: dict[AccessKey, Row] = {}
        async with self._db.begin_readonly() as conn:
            j = sa.join(kernels, users, kernels.c.user_uuid == users.c.uuid)
            query = (
                sa.select([
                    kernels.c.id,
                    kernels.c.access_key,
                    kernels.c.session_id,
                    kernels.c.session_type,
                    kernels.c.created_at,
                    kernels.c.occupied_slots,
                    kernels.c.cluster_size,
                    users.c.created_at.label("user_created_at"),
                ])
                .select_from(j)
                .where(
                    (kernels.c.status.in_(LIVE_STATUS))
                    & (kernels.c.cluster_role == DEFAULT_ROLE)
                    & (kernels.c.session_type != SessionTypes.INFERENCE),
                )
            )
            result = await conn.execute(query)
            rows = result.fetchall()
            for kernel in rows:
                grace_period_end = await self._grace_period_checker.get_grace_period_end(kernel)
                policy = policy_cache.get(kernel["access_key"], None)
                if policy is None:
                    query = (
                        sa.select([
                            keypair_resource_policies.c.max_session_lifetime,
                            keypair_resource_policies.c.idle_timeout,
                        ])
                        .select_from(
                            sa.join(
                                keypairs,
                                keypair_resource_policies,
                                keypair_resource_policies.c.name == keypairs.c.resource_policy,
                            ),
                        )
                        .where(keypairs.c.access_key == kernel["access_key"])
                    )
                    result = await conn.execute(query)
                    policy = result.first()
                    assert policy is not None
                    policy_cache[kernel["access_key"]] = policy

                check_task = [
                    checker.check_idleness(
                        kernel, conn, policy, self._redis_live, grace_period_end=grace_period_end
                    )
                    for checker in self._checkers
                ]
                check_results = await asyncio.gather(*check_task, return_exceptions=True)
                terminated = False
                errors = []

class AbstractIdleCheckReporter(metaclass=ABCMeta):
    remaining_time_type: RemainingTimeType
    name: ClassVar[str] = "base"
    report_key: ClassVar[str] = "base"
    extra_info_key: ClassVar[str] = "base_extra"

    def __init__(
        self,
        event_dispatcher: EventDispatcher,
        redis_live: RedisConnectionInfo,
        redis_stat: RedisConnectionInfo,
    ) -> None:
        self._event_dispatcher = event_dispatcher
        self._redis_live = redis_live
        self._redis_stat = redis_stat

    async def aclose(self) -> None:
        pass

    @abstractmethod
    async def populate_config(self, config: Mapping[str, Any]) -> None:
        raise NotImplementedError

    async def update_app_streaming_status(
        self,
        session_id: SessionId,
        status: AppStreamingStatus,
    ) -> None:
        pass

    @classmethod
    def get_report_key(cls, session_id: SessionId) -> str:
        return f"session.{session_id}.{cls.name}.report"

    @abstractmethod
    async def get_extra_info(self, session_id: SessionId) -> Optional[dict[str, Any]]:
        return None

    @abstractmethod
    async def get_checker_result(
        self,
        redis_obj: RedisConnectionInfo,
        session_id: SessionId,
    ) -> Optional[float]:
        pass

    async def set_remaining_time_report(
        self, redis_obj: RedisConnectionInfo, session_id: SessionId, remaining: float
    ) -> None:
        await redis_helper.execute(
            redis_obj,
            lambda r: r.set(
                self.get_report_key(session_id),
                msgpack.packb(remaining),
                ex=int(DEFAULT_CHECK_INTERVAL) * 10,
            ),
        )


class AbstractIdleChecker(metaclass=ABCMeta):
    terminate_reason: KernelLifecycleEventReason

    @abstractmethod
    async def check_idleness(
        self,
        kernel: Row,
        dbconn: SAConnection,
        policy: Row,
        redis_obj: RedisConnectionInfo,
        *,
        grace_period_end: Optional[datetime] = None,
    ) -> bool:

        return True


class NewUserGracePeriodChecker(AbstractIdleCheckReporter):
    remaining_time_type: RemainingTimeType = RemainingTimeType.GRACE_PERIOD
    name: ClassVar[str] = "user_grace_period"
    report_key: ClassVar[str] = "user_grace_period"
    user_initial_grace_period: Optional[timedelta] = None

    _config_iv = t.Dict(
        {
            t.Key("user_initial_grace_period", default=None): t.Null | tx.TimeDuration(),
        },
    ).allow_extra("*")

    async def populate_config(self, raw_config: Mapping[str, Any]) -> None:
        config = self._config_iv.check(raw_config)
        self.user_initial_grace_period = config["user_initial_grace_period"]
        _grace_period = (
            self.user_initial_grace_period.total_seconds()
            if self.user_initial_grace_period is not None
            else None
        )

        log.info(
            f"NewUserGracePeriodChecker: default period = {_grace_period} seconds",
        )

    async def get_extra_info(self, session_id: SessionId) -> Optional[dict[str, Any]]:
        return None

    async def del_remaining_time_report(
        self, redis_obj: RedisConnectionInfo, session_id: SessionId
    ) -> None:
        await redis_helper.execute(
            redis_obj,
            lambda r: r.delete(
                self.get_report_key(session_id),
            ),
        )

    async def get_grace_period_end(
        self,
        kernel: Row,
    ) -> Optional[datetime]:

        if self.user_initial_grace_period is None:
            return None
        user_created_at: datetime = kernel["user_created_at"]
        return user_created_at + self.user_initial_grace_period

    @property
    def grace_period_const(self) -> float:
        return (
            self.user_initial_grace_period.total_seconds()
            if self.user_initial_grace_period is not None
            else 0
        )

    async def get_checker_result(
        self,
        redis_obj: RedisConnectionInfo,
        session_id: SessionId,
    ) -> Optional[float]:
        key = self.get_report_key(session_id)
        data = await redis_helper.execute(redis_obj, lambda r: r.get(key))
        return msgpack.unpackb(data) if data is not None else None


class BaseIdleChecker(AbstractIdleChecker, AbstractIdleCheckReporter):
    pass


class NetworkTimeoutIdleChecker(BaseIdleChecker):

    terminate_reason: KernelLifecycleEventReason = KernelLifecycleEventReason.IDLE_TIMEOUT
    remaining_time_type: RemainingTimeType = RemainingTimeType.EXPIRE_AFTER
    name: ClassVar[str] = "network_timeout"
    report_key: ClassVar[str] = "network_timeout"
    extra_info_key: ClassVar[str] = "network_timeout_timeout_extra"

    _config_iv = t.Dict(
        {
            t.Key("threshold", default="10m"): tx.TimeDuration(),
        },
    ).allow_extra("*")

    idle_timeout: timedelta
    _evhandlers: List[EventHandler[None, AbstractEvent]]

    def __init__(
        self,
        event_dispatcher: EventDispatcher,
        redis_live: RedisConnectionInfo,
        redis_stat: RedisConnectionInfo,
    ) -> None:
        super().__init__(event_dispatcher, redis_live, redis_stat)
        d = self._event_dispatcher
        (d.subscribe(SessionStartedEvent, None, self._session_started_cb),)
        self._evhandlers = [
            d.consume(ExecutionStartedEvent, None, self._execution_started_cb),
            d.consume(ExecutionFinishedEvent, None, self._execution_exited_cb),
            d.consume(ExecutionTimeoutEvent, None, self._execution_exited_cb),
            d.consume(ExecutionCancelledEvent, None, self._execution_exited_cb),
        ]

    async def aclose(self) -> None:
        for _evh in self._evhandlers:
            self._event_dispatcher.unconsume(_evh)

    async def populate_config(self, raw_config: Mapping[str, Any]) -> None:
        config = self._config_iv.check(raw_config)
        self.idle_timeout = config["threshold"]
        log.info(
            "NetworkTimeoutIdleChecker: default idle_timeout = {0:,} seconds",
            self.idle_timeout.total_seconds(),
        )

    async def update_app_streaming_status(
        self,
        session_id: SessionId,
        status: AppStreamingStatus,
    ) -> None:
        if status == AppStreamingStatus.HAS_ACTIVE_CONNECTIONS:
            await self._disable_timeout(session_id)
        elif status == AppStreamingStatus.NO_ACTIVE_CONNECTIONS:
            await self._update_timeout(session_id)

    async def _disable_timeout(self, session_id: SessionId) -> None:
        log.debug(f"NetworkTimeoutIdleChecker._disable_timeout({session_id})")
        await redis_helper.execute(
            self._redis_live,
            lambda r: r.set(
                f"session.{session_id}.last_access",
                "0",
                xx=True,
            ),
        )

    async def _update_timeout(self, session_id: SessionId) -> None:
        log.debug(f"NetworkTimeoutIdleChecker._update_timeout({session_id})")
        t = await redis_helper.execute(self._redis_live, lambda r: r.time())
        t = t[0] + (t[1] / (10**6))
        await redis_helper.execute(
            self._redis_live,
            lambda r: r.set(
                f"session.{session_id}.last_access",
                f"{t:.06f}",
                ex=max(86400, int(self.idle_timeout.total_seconds() * 2)),
            ),
        )