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
        """
        Initialize an API request.

        :param BaseSession session: The session where this request is executed on.

        :param str path: The query path. When performing requests, the version number
                         prefix will be automatically prepended if required.

        :param RequestContent content: The API query body which will be encoded as
                                       JSON.

        :param str content_type: Explicitly set the content type.  See also
                                 :func:`Request.set_content`.
        """
        self.session = api_session.get()
        self.config = self.session.config
        self.method = method
        if path is not None and path.startswith("/"):
            path = path[1:]
        self.path = path
        self.params = params
        self.date = None
        if override_api_version:
            self.api_version = override_api_version
        else:
            self.api_version = f"v{self.session.api_version[0]}.{self.session.api_version[1]}"
        self.headers = CIMultiDict([
            ("User-Agent", self.config.user_agent),
            ("X-BackendAI-Domain", self.config.domain),
            ("X-BackendAI-Version", self.api_version),
        ])
        self._content = b""
        self._attached_files = None
        self.set_content(content, content_type=content_type)
        self.reporthook = reporthook

    @property
    def content(self) -> RequestContent:
        """
        Retrieves the content in the original form.
        Private codes should NOT use this as it incurs duplicate
        encoding/decoding.
        """
        return self._content

    def set_content(
        self,
        value: RequestContent,
        *,
        content_type: str = None,
    ) -> None:
        """
        Sets the content of the request.
        """
        assert (
            self._attached_files is None
        ), "cannot set content because you already attached files."
        guessed_content_type = "application/octet-stream"
        if value is None:
            guessed_content_type = "text/plain"
            self._content = b""
        elif isinstance(value, str):
            guessed_content_type = "text/plain"
            self._content = value.encode("utf-8")
        else:
            guessed_content_type = "application/octet-stream"
            self._content = value
        self.content_type = content_type if content_type is not None else guessed_content_type