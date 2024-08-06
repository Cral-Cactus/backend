from __future__ import annotations

import asyncio
import functools
import grp
import importlib
import importlib.resources
import logging
import os
import pwd
import ssl
import sys
import traceback
from contextlib import asynccontextmanager as actxmgr
from datetime import datetime
from pathlib import Path
from typing import (
    Any,
    AsyncIterator,
    Final,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Sequence,
    cast,
)

import aiohttp_cors
import aiomonitor
import aiotools
import click
from aiohttp import web
from setproctitle import setproctitle

from ai.backend.common import redis_helper
from ai.backend.common.auth import PublicKey, SecretKey
from ai.backend.common.bgtask import BackgroundTaskManager
from ai.backend.common.cli import LazyGroup

from ai.backend.common.events import EventDispatcher, EventProducer, KernelLifecycleEventReason
from ai.backend.common.events_experimental import EventDispatcher as ExperimentalEventDispatcher
from ai.backend.common.logging import BraceStyleAdapter, Logger
from ai.backend.common.plugin.hook import ALL_COMPLETED, PASSED, HookPluginContext
from ai.backend.common.plugin.monitor import INCREMENT
from ai.backend.common.types import AgentSelectionStrategy, LogSeverity
from ai.backend.common.utils import env_info

from . import __version__
from .agent_cache import AgentRPCCache
from .api import ManagerStatus
from .api.context import RootContext
from .api.exceptions import (
    BackendError,
    GenericBadRequest,
    InternalServerError,
    InvalidAPIParameters,
    MethodNotAllowed,
    URLNotFound,
)
from .api.types import (
    AppCreator,
    CleanupContext,
    WebMiddleware,
    WebRequestHandler,
)
from .config import LocalConfig, SharedConfig, volume_config_iv
from .config import load as load_config
from .exceptions import InvalidArgument
from .models import SessionRow
from .types import DistributedLockFactory

@web.middleware
async def api_middleware(request: web.Request, handler: WebRequestHandler) -> web.StreamResponse:
    _handler = handler
    method_override = request.headers.get("X-Method-Override", None)
    if method_override:
        request = request.clone(method=method_override)
        new_match_info = await request.app.router.resolve(request)
        if new_match_info is None:
            raise InternalServerError("No matching method handler found")
        _handler = new_match_info.handler
        request._match_info = new_match_info
    ex = request.match_info.http_exception
    if ex is not None:
        raise ex
    new_api_version = request.headers.get("X-Backend-Version")
    legacy_api_version = request.headers.get("X-Sorna-Version")
    api_version = new_api_version or legacy_api_version

@web.middleware
async def exception_middleware(
    request: web.Request, handler: WebRequestHandler
) -> web.StreamResponse:
    root_ctx: RootContext = request.app["_root.context"]
    error_monitor = root_ctx.error_monitor
    stats_monitor = root_ctx.stats_monitor
    try:
        await stats_monitor.report_metric(INCREMENT, "ai.backend.manager.api.requests")
        resp = await handler(request)
    except InvalidArgument as ex:
        if len(ex.args) > 1:
            raise InvalidAPIParameters(f"{ex.args[0]}: {', '.join(map(str, ex.args[1:]))}")
        elif len(ex.args) == 1:
            raise InvalidAPIParameters(ex.args[0])
        else:
            raise InvalidAPIParameters()
    except BackendError as ex:
        if ex.status_code == 500:
            log.warning("Internal server error raised inside handlers")
        await error_monitor.capture_exception()
        await stats_monitor.report_metric(INCREMENT, "ai.backend.manager.api.failures")
        await stats_monitor.report_metric(
            INCREMENT, f"ai.backend.manager.api.status.{ex.status_code}"
        )
        raise
    except web.HTTPException as ex:
        await stats_monitor.report_metric(INCREMENT, "ai.backend.manager.api.failures")
        await stats_monitor.report_metric(
            INCREMENT, f"ai.backend.manager.api.status.{ex.status_code}"
        )
        if ex.status_code == 404:
            raise URLNotFound(extra_data=request.path)
        if ex.status_code == 405:
            concrete_ex = cast(web.HTTPMethodNotAllowed, ex)
            raise MethodNotAllowed(
                method=concrete_ex.method, allowed_methods=concrete_ex.allowed_methods
            )
        log.warning("Bad request: {0!r}", ex)
        raise GenericBadRequest
    except asyncio.CancelledError as e:
        log.debug("Request cancelled ({0} {1})", request.method, request.rel_url)
        raise e
    except Exception as e:
        await error_monitor.capture_exception()
        log.exception("Uncaught exception in HTTP request handlers {0!r}", e)
        if root_ctx.local_config["debug"]["enabled"]:
            raise InternalServerError(traceback.format_exc())
        else:
            raise InternalServerError()
    else:
        await stats_monitor.report_metric(INCREMENT, f"ai.backend.manager.api.status.{resp.status}")
        return resp


@actxmgr
async def shared_config_ctx(root_ctx: RootContext) -> AsyncIterator[None]:
    root_ctx.shared_config = SharedConfig(
        root_ctx.local_config["etcd"]["addr"],
        root_ctx.local_config["etcd"]["user"],
        root_ctx.local_config["etcd"]["password"],
        root_ctx.local_config["etcd"]["namespace"],
    )
    await root_ctx.shared_config.reload()
    yield
    await root_ctx.shared_config.close()


@actxmgr
async def webapp_plugin_ctx(root_app: web.Application) -> AsyncIterator[None]:
    from .plugin.webapp import WebappPluginContext

    root_ctx: RootContext = root_app["_root.context"]
    plugin_ctx = WebappPluginContext(root_ctx.shared_config.etcd, root_ctx.local_config)
    await plugin_ctx.init(
        context=root_ctx,
        allowlist=root_ctx.local_config["manager"]["allowed-plugins"],
        blocklist=root_ctx.local_config["manager"]["disabled-plugins"],
    )
    root_ctx.webapp_plugin_ctx = plugin_ctx
    for plugin_name, plugin_instance in plugin_ctx.plugins.items():
        if root_ctx.pidx == 0:
            log.info("Loading webapp plugin: {0}", plugin_name)
        subapp, global_middlewares = await plugin_instance.create_app(root_ctx.cors_options)
        _init_subapp(plugin_name, root_app, subapp, global_middlewares)
    yield
    await plugin_ctx.cleanup()


@actxmgr
async def manager_status_ctx(root_ctx: RootContext) -> AsyncIterator[None]:
    if root_ctx.pidx == 0:
        mgr_status = await root_ctx.shared_config.get_manager_status()
        if mgr_status is None or mgr_status not in (ManagerStatus.RUNNING, ManagerStatus.FROZEN):
            await root_ctx.shared_config.update_manager_status(ManagerStatus.RUNNING)
            mgr_status = ManagerStatus.RUNNING
        log.info("Manager status: {}", mgr_status)
        tz = root_ctx.shared_config["system"]["timezone"]
        log.info("Configured timezone: {}", tz.tzname(datetime.now()))
    yield


@actxmgr
async def redis_ctx(root_ctx: RootContext) -> AsyncIterator[None]:
    root_ctx.shared_config.data["redis"]

    root_ctx.redis_live = redis_helper.get_redis_object(
        root_ctx.shared_config.data["redis"],
        name="live",
        db=REDIS_LIVE_DB,
    )
    root_ctx.redis_stat = redis_helper.get_redis_object(
        root_ctx.shared_config.data["redis"],
        name="stat",
        db=REDIS_STAT_DB,
    )
    root_ctx.redis_image = redis_helper.get_redis_object(
        root_ctx.shared_config.data["redis"],
        name="image",
        db=REDIS_IMAGE_DB,
    )
    root_ctx.redis_stream = redis_helper.get_redis_object(
        root_ctx.shared_config.data["redis"],
        name="stream",
        db=REDIS_STREAM_DB,
    )
    root_ctx.redis_lock = redis_helper.get_redis_object(
        root_ctx.shared_config.data["redis"],
        name="lock",
        db=REDIS_STREAM_LOCK,
    )
    for redis_info in (
        root_ctx.redis_live,
        root_ctx.redis_stat,
        root_ctx.redis_image,
        root_ctx.redis_stream,
        root_ctx.redis_lock,
    ):
        await redis_helper.ping_redis_connection(redis_info.client)
    yield
    await root_ctx.redis_stream.close()
    await root_ctx.redis_image.close()
    await root_ctx.redis_stat.close()
    await root_ctx.redis_live.close()
    await root_ctx.redis_lock.close()


@actxmgr
async def database_ctx(root_ctx: RootContext) -> AsyncIterator[None]:
    from .models.utils import connect_database

    async with connect_database(root_ctx.local_config) as db:
        root_ctx.db = db
        yield


@actxmgr
async def distributed_lock_ctx(root_ctx: RootContext) -> AsyncIterator[None]:
    root_ctx.distributed_lock_factory = init_lock_factory(root_ctx)
    yield


@actxmgr
async def event_dispatcher_ctx(root_ctx: RootContext) -> AsyncIterator[None]:
    event_dispatcher_cls: type[EventDispatcher] | type[ExperimentalEventDispatcher]
    if root_ctx.local_config["manager"].get("use-experimental-redis-event-dispatcher"):
        event_dispatcher_cls = ExperimentalEventDispatcher
    else:
        event_dispatcher_cls = EventDispatcher

    root_ctx.event_producer = await EventProducer.new(
        root_ctx.shared_config.data["redis"],
        db=REDIS_STREAM_DB,
    )
    root_ctx.event_dispatcher = await event_dispatcher_cls.new(
        root_ctx.shared_config.data["redis"],
        db=REDIS_STREAM_DB,
        log_events=root_ctx.local_config["debug"]["log-events"],
        consumer_group=EVENT_DISPATCHER_CONSUMER_GROUP,
        node_id=root_ctx.local_config["manager"]["id"],
    )
    yield
    await root_ctx.event_producer.close()
    await asyncio.sleep(0.2)
    await root_ctx.event_dispatcher.close()


@actxmgr
async def idle_checker_ctx(root_ctx: RootContext) -> AsyncIterator[None]:
    from .idle import init_idle_checkers

    root_ctx.idle_checker_host = await init_idle_checkers(
        root_ctx.db,
        root_ctx.shared_config,
        root_ctx.event_dispatcher,
        root_ctx.event_producer,
        root_ctx.distributed_lock_factory,
    )
    await root_ctx.idle_checker_host.start()
    yield
    await root_ctx.idle_checker_host.shutdown()


@actxmgr
async def hook_plugin_ctx(root_ctx: RootContext) -> AsyncIterator[None]:
    ctx = HookPluginContext(root_ctx.shared_config.etcd, root_ctx.local_config)
    root_ctx.hook_plugin_ctx = ctx
    await ctx.init(
        context=root_ctx,
        allowlist=root_ctx.local_config["manager"]["allowed-plugins"],
        blocklist=root_ctx.local_config["manager"]["disabled-plugins"],
    )
    hook_result = await ctx.dispatch(
        "ACTIVATE_MANAGER",
        (),
        return_when=ALL_COMPLETED,
    )
    if hook_result.status != PASSED:
        raise RuntimeError("Could not activate the manager instance.")
    yield
    await ctx.cleanup()


@actxmgr
async def agent_registry_ctx(root_ctx: RootContext) -> AsyncIterator[None]:
    from zmq.auth.certs import load_certificate

    from .registry import AgentRegistry

    manager_pkey, manager_skey = load_certificate(
        root_ctx.local_config["manager"]["rpc-auth-manager-keypair"]
    )
    assert manager_skey is not None
    manager_public_key = PublicKey(manager_pkey)
    manager_secret_key = SecretKey(manager_skey)
    root_ctx.agent_cache = AgentRPCCache(root_ctx.db, manager_public_key, manager_secret_key)
    root_ctx.registry = AgentRegistry(
        root_ctx.local_config,
        root_ctx.shared_config,
        root_ctx.db,
        root_ctx.agent_cache,
        root_ctx.redis_stat,
        root_ctx.redis_live,
        root_ctx.redis_image,
        root_ctx.redis_stream,
        root_ctx.event_dispatcher,
        root_ctx.event_producer,
        root_ctx.storage_manager,
        root_ctx.hook_plugin_ctx,
        debug=root_ctx.local_config["debug"]["enabled"],
        manager_public_key=manager_public_key,
        manager_secret_key=manager_secret_key,
    )
    await root_ctx.registry.init()
    yield
    await root_ctx.registry.shutdown()


@actxmgr
async def sched_dispatcher_ctx(root_ctx: RootContext) -> AsyncIterator[None]:
    from .scheduler.dispatcher import SchedulerDispatcher

    sched_dispatcher = await SchedulerDispatcher.new(
        root_ctx.local_config,
        root_ctx.shared_config,
        root_ctx.event_dispatcher,
        root_ctx.event_producer,
        root_ctx.distributed_lock_factory,
        root_ctx.registry,
    )
    yield
    await sched_dispatcher.close()


@actxmgr
async def monitoring_ctx(root_ctx: RootContext) -> AsyncIterator[None]:
    from .plugin.monitor import ManagerErrorPluginContext, ManagerStatsPluginContext

    ectx = ManagerErrorPluginContext(root_ctx.shared_config.etcd, root_ctx.local_config)
    sctx = ManagerStatsPluginContext(root_ctx.shared_config.etcd, root_ctx.local_config)
    init_success = False
    try:
        await ectx.init(
            context={"_root.context": root_ctx},
            allowlist=root_ctx.local_config["manager"]["allowed-plugins"],
        )
        await sctx.init(allowlist=root_ctx.local_config["manager"]["allowed-plugins"])
    except Exception:
        log.error("Failed to initialize monitoring plugins")
    else:
        init_success = True
        root_ctx.error_monitor = ectx
        root_ctx.stats_monitor = sctx
    yield
    if init_success:
        await sctx.cleanup()
        await ectx.cleanup()


@actxmgr
async def hanging_session_scanner_ctx(root_ctx: RootContext) -> AsyncIterator[None]:
    from contextlib import suppress
    from datetime import timedelta
    from typing import TYPE_CHECKING

    import sqlalchemy as sa
    from dateutil.relativedelta import relativedelta
    from dateutil.tz import tzutc
    from sqlalchemy.orm import load_only, noload

    from .config import session_hang_tolerance_iv
    from .models.session import SessionStatus

    if TYPE_CHECKING:
        from .models.utils import ExtendedAsyncSAEngine

    async def _fetch_hanging_sessions(
        db: ExtendedAsyncSAEngine,
        status: SessionStatus,
        threshold: relativedelta | timedelta,
    ) -> tuple[SessionRow, ...]:
        query = (
            sa.select(SessionRow)
            .where(SessionRow.status == status)
            .where(
                (
                    datetime.now(tz=tzutc())
                    - SessionRow.status_history[status.name].astext.cast(
                        sa.types.DateTime(timezone=True)
                    )
                )
                > threshold
            )
            .options(
                noload("*"),
                load_only(SessionRow.id, SessionRow.name, SessionRow.status, SessionRow.access_key),
            )
        )
        async with db.begin_readonly() as conn:
            result = await conn.execute(query)
            return result.fetchall()

    async def _force_terminate_hanging_sessions(
        status: SessionStatus,
        threshold: relativedelta | timedelta,
        interval: float,
    ) -> None:
        try:
            sessions = await _fetch_hanging_sessions(root_ctx.db, status, threshold)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.error("fetching hanging sessions error: {}", repr(e), exc_info=e)
            return

        log.debug(f"{len(sessions)} {status.name} sessions found.")

        results_and_exceptions = await asyncio.gather(
            *[
                asyncio.create_task(
                    root_ctx.registry.destroy_session(
                        session, forced=True, reason=KernelLifecycleEventReason.HANG_TIMEOUT
                    ),
                )
                for session in sessions
            ],
            return_exceptions=True,
        )
        for result_or_exception in results_and_exceptions:
            if isinstance(result_or_exception, (BaseException, Exception)):
                log.error(
                    "hanging session force-termination error: {}",
                    repr(result_or_exception),
                    exc_info=result_or_exception,
                )

    session_hang_tolerance = session_hang_tolerance_iv.check(
        await root_ctx.shared_config.etcd.get_prefix_dict("config/session/hang-tolerance")
    )

    session_force_termination_tasks = []
    heuristic_interval_weight = 0.4
    max_interval = timedelta(hours=1).total_seconds()
    threshold: relativedelta | timedelta
    for status, threshold in session_hang_tolerance["threshold"].items():
        try:
            session_status = SessionStatus[status]
        except KeyError:
            continue
        if isinstance(threshold, relativedelta):
            interval = max_interval
        else:  # timedelta
            interval = min(max_interval, threshold.total_seconds() * heuristic_interval_weight)
        session_force_termination_tasks.append(
            aiotools.create_timer(
                functools.partial(_force_terminate_hanging_sessions, session_status, threshold),
                interval,
            )
        )

    yield

    for task in session_force_termination_tasks:
        if not task.done():
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task


class background_task_ctx:
    def __init__(self, root_ctx: RootContext) -> None:
        self.root_ctx = root_ctx

    async def __aenter__(self) -> None:
        self.root_ctx.background_task_manager = BackgroundTaskManager(self.root_ctx.event_producer)

    async def __aexit__(self, *exc_info) -> None:
        pass

    async def shutdown(self) -> None:
        if hasattr(self.root_ctx, "background_task_manager"):
            await self.root_ctx.background_task_manager.shutdown()