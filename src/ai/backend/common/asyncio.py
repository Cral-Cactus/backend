from __future__ import annotations

import asyncio
import inspect
from typing import (
    Any,
    Awaitable,
    Callable,
    Collection,
    Sequence,
    Tuple,
    Type,
    cast,
)

__all__ = (
    "AsyncBarrier",
    "cancel_tasks",
    "current_loop",
    "run_through",
)


async def cancel_tasks(
    tasks: Collection[asyncio.Task[Any]],
) -> Sequence[Any]:
    copied_tasks = {*tasks}
    cancelled_tasks = []
    for task in copied_tasks:
        if not task.done():
            task.cancel()
            cancelled_tasks.append(task)
    return await asyncio.gather(*cancelled_tasks, return_exceptions=True)


current_loop: Callable[[], asyncio.AbstractEventLoop]
if hasattr(asyncio, "get_running_loop"):
    current_loop = asyncio.get_running_loop  # type: ignore
else:
    current_loop = asyncio.get_event_loop  # type: ignore


async def run_through(
    *awaitable_or_callables: Callable[[], None] | Awaitable[None],
    ignored_exceptions: Tuple[Type[Exception], ...],
) -> None:
    for f in awaitable_or_callables:
        try:
            if inspect.iscoroutinefunction(f):
                await f()  # type: ignore
            elif inspect.isawaitable(f):
                await f  # type: ignore
            else:
                f()  # type: ignore
        except Exception as e:
            if isinstance(e, cast(Tuple[Any, ...], ignored_exceptions)):
                continue
            raise


class AsyncBarrier:
    """
    This class provides a simplified asyncio-version of threading.Barrier class.
    """

    num_parties: int = 1
    cond: asyncio.Condition

    def __init__(self, num_parties: int) -> None:
        self.num_parties = num_parties
        self.count = 0
        self.cond = asyncio.Condition()

    async def wait(self) -> None:
        async with self.cond:
            self.count += 1
            if self.count == self.num_parties:
                self.cond.notify_all()
            else:
                while self.count < self.num_parties:
                    await self.cond.wait()

    def reset(self) -> None:
        self.count = 0