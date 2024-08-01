from __future__ import annotations

import asyncio
import functools
import io
import json as modjson
import logging
import sys
from collections import OrderedDict, namedtuple
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    List,
    Mapping,
    Optional,
    Sequence,
    Type,
    TypeVar,
    Union,
    cast,
)

import aiohttp
import aiohttp.web
import appdirs
import attrs
from aiohttp.client import _RequestContextManager, _WSRequestContextManager
from dateutil.tz import tzutc
from multidict import CIMultiDict
from yarl import URL

from .auth import generate_signature
from .exceptions import BackendAPIError, BackendClientError
from .session import AsyncSession, BaseSession, api_session
from .session import Session as SyncSession

log = logging.getLogger(__spec__.name)  # type: ignore[name-defined]

__all__ = [
    "Request",
    "BaseResponse",
    "Response",
    "WebSocketResponse",
    "SSEResponse",
    "FetchContextManager",
    "WebSocketContextManager",
    "SSEContextManager",
    "AttachedFile",
]


RequestContent = Union[
    bytes,
    bytearray,
    str,
    aiohttp.StreamReader,
    io.IOBase,
    None,
]
"""
The type alias for the set of allowed types for request content.
"""


AttachedFile = namedtuple("AttachedFile", "filename stream content_type")
"""
A struct that represents an attached file to the API request.

:param str filename: The name of file to store. It may include paths
                     and the server will create parent directories
                     if required.

:param Any stream: A file-like object that allows stream-reading bytes.

:param str content_type: The content type for the stream.  For arbitrary
                         binary data, use "application/octet-stream".
"""


_T = TypeVar("_T")


async def _coro_return(val: _T) -> _T:
    return val


class ExtendedJSONEncoder(modjson.JSONEncoder):
    def default(self, obj: Any) -> Any:
        if isinstance(obj, Path):
            return str(obj)
        if isinstance(obj, Decimal):
            return str(obj)
        return super().default(obj)


class Request:
    """
    The API request object.
    """

    __slots__ = (
        "config",
        "session",
        "method",
        "path",
        "date",
        "headers",
        "params",
        "content_type",
        "api_version",
        "_content",
        "_attached_files",
        "reporthook",
    )

    _content: RequestContent
    _attached_files: Optional[Sequence[AttachedFile]]

    date: Optional[datetime]
    api_version: str

    _allowed_methods = frozenset(["GET", "HEAD", "POST", "PUT", "PATCH", "DELETE", "OPTIONS"])

    def __init__(
        self,
        method: str = "GET",
        path: str = None,
        content: RequestContent = None,
        *,
        content_type: str = None,
        params: Mapping[str, Union[str, int]] = None,
        reporthook: Callable = None,
        override_api_version: str = None,
    ) -> None:

           def set_json(self, value: Any) -> None:
        """
        A shortcut for set_content() with JSON objects.
        """
        self.set_content(
            modjson.dumps(value, cls=ExtendedJSONEncoder), content_type="application/json"
        )

    def attach_files(self, files: Sequence[AttachedFile]) -> None:
        """
        Attach a list of files represented as AttachedFile.
        """
        assert not self._content, "content must be empty to attach files."
        self.content_type = "multipart/form-data"
        self._attached_files = files

    def _sign(
        self,
        rel_url: URL,
        access_key: str = None,
        secret_key: str = None,
        hash_type: str = None,
    ) -> None:
        """
        Calculates the signature of the given request and adds the
        Authorization HTTP header.
        It should be called at the very end of request preparation and before
        sending the request to the server.
        """
        if access_key is None:
            access_key = self.config.access_key
        if secret_key is None:
            secret_key = self.config.secret_key
        if hash_type is None:
            hash_type = self.config.hash_type
        assert self.date is not None
        if self.config.endpoint_type == "api":
            hdrs, _ = generate_signature(
                method=self.method,
                version=self.api_version,
                endpoint=self.config.endpoint,
                date=self.date,
                rel_url=str(rel_url),
                content_type=self.content_type,
                access_key=access_key,
                secret_key=secret_key,
                hash_type=hash_type,
            )
            self.headers.update(hdrs)
        elif self.config.endpoint_type == "session":
            local_state_path = Path(appdirs.user_state_dir("backend", "Lablup"))
            try:
                cookie_jar = cast(aiohttp.CookieJar, self.session.aiohttp_session.cookie_jar)
                cookie_jar.load(local_state_path / "cookie.dat")
            except (IOError, PermissionError):
                pass
        else:
            raise ValueError("unsupported endpoint type")

    def _pack_content(self) -> Union[RequestContent, aiohttp.FormData]:
        if self._attached_files is not None:
            data = aiohttp.FormData()
            for f in self._attached_files:
                data.add_field("src", f.stream, filename=f.filename, content_type=f.content_type)
            assert data.is_multipart, "Failed to pack files as multipart."
            # Let aiohttp fill up the content-type header including
            # multipart boundaries.
            self.headers.pop("Content-Type", None)
            return data
        else:
            return self._content

    def _build_url(self) -> URL:
        base_url = self.config.endpoint.path.rstrip("/")
        query_path = self.path.lstrip("/") if self.path is not None and len(self.path) > 0 else ""
        if self.config.endpoint_type == "session":
            if not query_path.startswith("server"):
                query_path = "func/{0}".format(query_path)
        path = "{0}/{1}".format(base_url, query_path)
        url = self.config.endpoint.with_path(path)
        if self.params:
            url = url.with_query(self.params)
        return url

    # TODO: attach rate-limit information

    def fetch(self, **kwargs) -> FetchContextManager:
        """
        Sends the request to the server and reads the response.

        You may use this method with AsyncSession only,
        following the pattern below:

        .. code-block:: python3

          from ai.backend.client.request import Request
          from ai.backend.client.session import AsyncSession

          async with AsyncSession() as sess:
            rqst = Request('GET', ...)
            async with rqst.fetch() as resp:
              print(await resp.text())
        """
        assert self.method in self._allowed_methods, "Disallowed HTTP method: {}".format(
            self.method
        )
        self.date = datetime.now(tzutc())
        assert self.date is not None
        self.headers["Date"] = self.date.isoformat()
        if self.content_type is not None and "Content-Type" not in self.headers:
            self.headers["Content-Type"] = self.content_type
        force_anonymous = kwargs.pop("anonymous", False)

        def _rqst_ctx_builder():
            timeout_config = aiohttp.ClientTimeout(
                total=None,
                connect=None,
                sock_connect=self.config.connection_timeout,
                sock_read=self.config.read_timeout,
            )
            full_url = self._build_url()
            if not self.config.is_anonymous and not force_anonymous:
                self._sign(full_url.relative())
            return self.session.aiohttp_session.request(
                self.method,
                str(full_url),
                data=self._pack_content(),
                timeout=timeout_config,
                headers=self.headers,
                allow_redirects=False,
            )

        return FetchContextManager(self.session, _rqst_ctx_builder, **kwargs)

    def connect_websocket(self, **kwargs) -> WebSocketContextManager:
        """
        Creates a WebSocket connection.

        .. warning::

          This method only works with
          :class:`~ai.backend.client.session.AsyncSession`.
        """
        assert isinstance(
            self.session, AsyncSession
        ), "Cannot use websockets with sessions in the synchronous mode"
        assert self.method == "GET", "Invalid websocket method"
        self.date = datetime.now(tzutc())
        assert self.date is not None
        self.headers["Date"] = self.date.isoformat()
        # websocket is always a "binary" stream.
        self.content_type = "application/octet-stream"

        def _ws_ctx_builder():
            full_url = self._build_url()
            if not self.config.is_anonymous:
                self._sign(full_url.relative())
            return self.session.aiohttp_session.ws_connect(
                str(full_url), autoping=True, heartbeat=30.0, headers=self.headers
            )

        return WebSocketContextManager(self.session, _ws_ctx_builder, **kwargs)

    def connect_events(self, **kwargs) -> SSEContextManager:
        """
        Creates a Server-Sent Events connection.

        .. warning::

          This method only works with
          :class:`~ai.backend.client.session.AsyncSession`.
        """
        assert isinstance(
            self.session, AsyncSession
        ), "Cannot use event streams with sessions in the synchronous mode"
        assert self.method == "GET", "Invalid event stream method"
        self.date = datetime.now(tzutc())
        assert self.date is not None
        self.headers["Date"] = self.date.isoformat()
        self.content_type = "application/octet-stream"

        def _rqst_ctx_builder():
            timeout_config = aiohttp.ClientTimeout(
                total=None,
                connect=None,
                sock_connect=self.config.connection_timeout,
                sock_read=self.config.read_timeout,
            )
            full_url = self._build_url()
            if not self.config.is_anonymous:
                self._sign(full_url.relative())
            return self.session.aiohttp_session.request(
                self.method, str(full_url), timeout=timeout_config, headers=self.headers
            )

        return SSEContextManager(self.session, _rqst_ctx_builder, **kwargs)


class AsyncResponseMixin:
    _session: BaseSession
    _raw_response: aiohttp.ClientResponse

    async def text(self) -> str:
        return await self._raw_response.text()

    async def json(self, *, loads=modjson.loads) -> Any:
        loads = functools.partial(loads, object_pairs_hook=OrderedDict)
        return await self._raw_response.json(loads=loads)

    async def read(self, n: int = -1) -> bytes:
        return await self._raw_response.content.read(n)

    async def readall(self) -> bytes:
        return await self._raw_response.content.read(-1)


class SyncResponseMixin:
    _session: BaseSession
    _raw_response: aiohttp.ClientResponse

    def text(self) -> str:
        sync_session = cast(SyncSession, self._session)
        return sync_session.worker_thread.execute(
            self._raw_response.text(),
        )