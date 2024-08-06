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
                for checker, result in zip(self._checkers, check_results):
                    if isinstance(result, aiotools.TaskGroupError):
                        errors.extend(result.__errors__)
                        continue
                    elif isinstance(result, Exception):
                        errors.append(result)
                        continue
                    if not result:
                        log.info(
                            "The {} idle checker triggered termination of s:{}",
                            checker.name,
                            kernel["session_id"],
                        )
                        if not terminated:
                            terminated = True
                            await self._event_producer.produce_event(
                                DoTerminateSessionEvent(
                                    kernel["session_id"],
                                    checker.terminate_reason,
                                ),
                            )
                if errors:
                    raise IdleCheckerError("idle checker(s) raise errors", errors)

    async def get_idle_check_report(
        self,
        session_id: SessionId,
    ) -> dict[str, Any]:
        return {
            checker.name: {
                "remaining": await checker.get_checker_result(self._redis_live, session_id),
                "remaining_time_type": checker.remaining_time_type.value,
                "extra": await checker.get_extra_info(session_id),
            }
            for checker in self._checkers
        }