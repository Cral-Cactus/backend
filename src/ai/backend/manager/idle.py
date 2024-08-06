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

class UtilizationResourceReport(UserDict):
    __slots__ = ("data",)

    data: dict[str, UtilizationExtraInfo]

    @classmethod
    def from_avg_threshold(
        cls,
        avg_utils: Mapping[str, float],
        thresholds: Mapping[str, Union[int, float, Decimal, None]],
        exclusions: set[str],
    ) -> UtilizationResourceReport:
        data: dict[str, UtilizationExtraInfo] = {
            k: UtilizationExtraInfo(float(avg_utils[k]), float(threshold))
            for k, threshold in thresholds.items()
            if (threshold is not None) and (k not in exclusions)
        }
        return cls(data)