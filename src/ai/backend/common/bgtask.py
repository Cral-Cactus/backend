from __future__ import annotations

import asyncio
import json
import logging
import time
import uuid
import weakref
from collections import defaultdict
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    DefaultDict,
    Final,
    Literal,
    Set,
    Type,
    TypeAlias,
    Union,
)

from aiohttp import web
from aiohttp_sse import sse_response
from redis.asyncio import Redis
from redis.asyncio.client import Pipeline

from . import redis_helper
from .events import (
    BgtaskCancelledEvent,
    BgtaskDoneEvent,
    BgtaskFailedEvent,
    BgtaskUpdatedEvent,
    EventDispatcher,
    EventProducer,
)
from .logging import BraceStyleAdapter
from .types import AgentId, Sentinel

sentinel: Final = Sentinel.TOKEN
log = BraceStyleAdapter(logging.getLogger(__spec__.name))  # type: ignore[name-defined]
TaskStatus = Literal["bgtask_started", "bgtask_done", "bgtask_cancelled", "bgtask_failed"]
BgtaskEvents: TypeAlias = (
    BgtaskUpdatedEvent | BgtaskDoneEvent | BgtaskCancelledEvent | BgtaskFailedEvent
)

MAX_BGTASK_ARCHIVE_PERIOD: Final = 86400  # 24  hours


class ProgressReporter:
    event_producer: Final[EventProducer]
    task_id: Final[uuid.UUID]
    total_progress: Union[int, float]
    current_progress: Union[int, float]

    def __init__(
        self,
        event_dispatcher: EventProducer,
        task_id: uuid.UUID,
        current_progress: int = 0,
        total_progress: int = 0,
    ) -> None:
        self.event_producer = event_dispatcher
        self.task_id = task_id
        self.current_progress = current_progress
        self.total_progress = total_progress

    async def update(
        self,
        increment: Union[int, float] = 0,
        message: str | None = None,
    ) -> None:
        self.current_progress += increment

        current, total = self.current_progress, self.total_progress
        redis_producer = self.event_producer.redis_client

        async def _pipe_builder(r: Redis) -> Pipeline:
            pipe = r.pipeline(transaction=False)
            tracker_key = f"bgtask.{self.task_id}"
            await pipe.hset(
                tracker_key,
                mapping={
                    "current": str(current),
                    "total": str(total),
                    "msg": message or "",
                    "last_update": str(time.time()),
                },
            )
            await pipe.expire(tracker_key, MAX_BGTASK_ARCHIVE_PERIOD)
            return pipe

        await redis_helper.execute(redis_producer, _pipe_builder)
        await self.event_producer.produce_event(
            BgtaskUpdatedEvent(
                self.task_id,
                message=message,
                current_progress=current,
                total_progress=total,
            ),
        )


BackgroundTask = Callable[[ProgressReporter], Awaitable[str | None]]


class BackgroundTaskManager:
    event_producer: EventProducer
    ongoing_tasks: weakref.WeakSet[asyncio.Task]
    task_update_queues: DefaultDict[uuid.UUID, Set[asyncio.Queue[Sentinel | BgtaskEvents]]]
    dict_lock: asyncio.Lock

    def __init__(self, event_producer: EventProducer) -> None:
        self.event_producer = event_producer
        self.ongoing_tasks = weakref.WeakSet()
        self.task_update_queues = defaultdict(set)
        self.dict_lock = asyncio.Lock()

    def register_event_handlers(self, event_dispatcher: EventDispatcher) -> None:

        event_dispatcher.subscribe(BgtaskUpdatedEvent, None, self._enqueue_bgtask_status_update)
        event_dispatcher.subscribe(BgtaskDoneEvent, None, self._enqueue_bgtask_status_update)
        event_dispatcher.subscribe(BgtaskCancelledEvent, None, self._enqueue_bgtask_status_update)
        event_dispatcher.subscribe(BgtaskFailedEvent, None, self._enqueue_bgtask_status_update)

    async def _enqueue_bgtask_status_update(
        self,
        context: None,
        source: AgentId,
        event: BgtaskEvents,
    ) -> None:
        task_id = event.task_id
        for q in self.task_update_queues[task_id]:
            q.put_nowait(event)

    async def push_bgtask_events(
        self,
        request: web.Request,
        task_id: uuid.UUID,
    ) -> web.StreamResponse:

        async with sse_response(request) as resp:
            try:
                async for event, extra_data in self.poll_bgtask_event(task_id):
                    body: dict[str, Any] = {
                        "task_id": str(event.task_id),
                        "message": event.message,
                    }
                    match event:
                        case BgtaskUpdatedEvent():
                            body["current_progress"] = event.current_progress
                            body["total_progress"] = event.total_progress
                            await resp.send(json.dumps(body), event=event.name, retry=5)
                        case BgtaskDoneEvent():
                            if extra_data:
                                body.update(extra_data)
                                await resp.send(
                                    json.dumps(body), event="bgtask_" + extra_data["status"]
                                )
                            else:
                                await resp.send("{}", event="bgtask_done")
                            await resp.send("{}", event="server_close")
                        case BgtaskCancelledEvent() | BgtaskFailedEvent():
                            await resp.send("{}", event="server_close")
            except:
                log.exception("")
                raise
            finally:
                return resp