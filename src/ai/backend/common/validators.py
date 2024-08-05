import datetime
import enum
import ipaddress
import json
import os
import pwd
import random
import re
import uuid
from collections.abc import Iterable
from decimal import Decimal
from pathlib import Path as _Path
from pathlib import PurePath as _PurePath
from typing import (
    Any,
    Generic,
    List,
    Literal,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    Union,
)

import dateutil.tz
from dateutil.relativedelta import relativedelta

try:
    import jwt

    jwt_available = True
except ImportError:
    jwt_available = False
import multidict
import trafaret as t
import yarl
from trafaret.base import TrafaretMeta, ensure_trafaret
from trafaret.lib import _empty

from .types import BinarySize as _BinarySize
from .types import HostPortPair as _HostPortPair
from .types import QuotaScopeID as _QuotaScopeID
from .types import RoundRobinState, RoundRobinStates
from .types import VFolderID as _VFolderID

__all__ = (
    "AliasedKey",
    "MultiKey",
    "BinarySize",
    "DelimiterSeperatedList",
    "StringList",
    "Enum",
    "JSONString",
)


class StringLengthMeta(TrafaretMeta):
    def __getitem__(cls, slice_):
        return cls(min_length=slice_.start, max_length=slice_.stop)


class AliasedKey(t.Key):
    def __init__(self, names: Sequence[str], **kwargs) -> None:
        super().__init__(names[0], **kwargs)
        self.names = names

        def __call__(self, data, context=None):
        for name in self.names:
            if name in data:
                key = name
                break
        else:
            key = None

class MultiKey(t.Key):
    def get_data(self, data, default):
        if isinstance(data, (multidict.MultiDict, multidict.MultiDictProxy)):
            return data.getall(self.name, default)
        raw_value = data.get(self.name, default)
        if isinstance(raw_value, (List, Tuple)):
            return raw_value
        return [raw_value]


class BinarySize(t.Trafaret):
    def check_and_return(self, value: Any) -> Union[_BinarySize, Decimal]:
        try:
            if not isinstance(value, str):
                value = str(value)
            return _BinarySize.from_str(value)
        except ValueError:
            self._failure("value is not a valid binary size", value=value)


TListItem = TypeVar("TListItem")


class DelimiterSeperatedList(t.Trafaret, Generic[TListItem]):
    def __init__(
        self,
        trafaret: Type[t.Trafaret] | t.Trafaret,
        *,
        delimiter: str = ",",
        min_length: Optional[int] = None,
        empty_str_as_empty_list: bool = False,
    ) -> None:
        self.delimiter = delimiter
        self.empty_str_as_empty_list = empty_str_as_empty_list
        self.min_length = min_length
        self.trafaret = ensure_trafaret(trafaret)

    def check_and_return(self, value: Any) -> Sequence[TListItem]:
        try:
            if not isinstance(value, str):
                value = str(value)
            if self.empty_str_as_empty_list and not value:
                return []
            splited = value.split(self.delimiter)
            if self.min_length is not None and len(splited) < self.min_length:
                self._failure(
                    f"the number of items should be greater than {self.min_length}",
                    value=value,
                )
            return [self.trafaret.check_and_return(x) for x in splited]
        except ValueError:
            self._failure("value is not a string or not convertible to string", value=value)


class StringList(DelimiterSeperatedList[str]):
    def __init__(
        self,
        *,
        delimiter: str = ",",
        allow_blank: bool = False,
        min_length: Optional[int] = None,
        empty_str_as_empty_list: bool = False,
    ) -> None:
        super().__init__(
            t.String(allow_blank=allow_blank),
            delimiter=delimiter,
            min_length=min_length,
            empty_str_as_empty_list=empty_str_as_empty_list,
        )


T_enum = TypeVar("T_enum", bound=enum.Enum)


class Enum(t.Trafaret, Generic[T_enum]):
    def __init__(self, enum_cls: Type[T_enum], *, use_name: bool = False) -> None:
        self.enum_cls = enum_cls
        self.use_name = use_name

    def check_and_return(self, value: Any) -> T_enum:
        try:
            if self.use_name:
                return self.enum_cls[value]
            else:
                return self.enum_cls(value)
        except (KeyError, ValueError):
            self._failure(f"value is not a valid member of {self.enum_cls.__name__}", value=value)


class JSONString(t.Trafaret):
    def check_and_return(self, value: Any) -> dict:
        try:
            return json.loads(value)
        except (KeyError, ValueError):
            self._failure("value is not a valid JSON string", value=value)


class PurePath(t.Trafaret):
    def __init__(
        self,
        *,
        base_path: _PurePath = None,
        relative_only: bool = False,
    ) -> None:
        super().__init__()
        self._base_path = base_path
        self._relative_only = relative_only

    def check_and_return(self, value: Any) -> _PurePath:
        try:
            p = _PurePath(value)
        except (TypeError, ValueError):
            self._failure("cannot parse value as a path", value=value)

        if self._relative_only and p.is_absolute():
            self._failure("expected relative path but the value is absolute", value=value)
        if self._base_path is not None:
            try:
                p.relative_to(self._base_path)
            except ValueError:
                self._failure("value is not in the base path", value=value)
        return p


class Path(PurePath):
    def __init__(
        self,
        *,
        type: Literal["dir", "file"],
        base_path: _Path = None,
        auto_create: bool = False,
        allow_nonexisting: bool = False,
        allow_devnull: bool = False,
        relative_only: bool = False,
        resolve: bool = True,
    ) -> None:
        super().__init__(
            base_path=base_path,
            relative_only=relative_only,
        )
        self._type = type
        if auto_create and type != "dir":
            raise TypeError("Only directory paths can be set auto-created.")
        self._auto_create = auto_create
        self._allow_nonexisting = allow_nonexisting
        self._allow_devnull = allow_devnull
        self._resolve = resolve

    def check_and_return(self, value: Any) -> _Path:
        try:
            p = _Path(value).resolve() if self._resolve else _Path(value)
        except (TypeError, ValueError):
            self._failure("cannot parse value as a path", value=value)
        if self._relative_only and p.is_absolute():
            self._failure("expected relative path but the value is absolute", value=value)
        if self._base_path is not None:
            try:
                _base_path = _Path(self._base_path).resolve() if self._resolve else self._base_path
                p.relative_to(_base_path)
            except ValueError:
                self._failure("value is not in the base path", value=value)
        if self._type == "dir":
            if self._auto_create:
                p.mkdir(parents=True, exist_ok=True)
            if not self._allow_nonexisting and not p.is_dir():
                self._failure("value is not a directory", value=value)
        elif self._type == "file":
            if not self._allow_devnull and str(p) == os.devnull:
                return p
            if not self._allow_nonexisting and not p.is_file():
                self._failure("value is not a regular file", value=value)
        return p


class IPNetwork(t.Trafaret):
    def check_and_return(self, value: Any) -> ipaddress._BaseNetwork:
        try:
            return ipaddress.ip_network(value)
        except ValueError:
            self._failure("Invalid IP network format", value=value)


class IPAddress(t.Trafaret):
    def check_and_return(self, value: Any) -> ipaddress._BaseAddress:
        try:
            return ipaddress.ip_address(value)
        except ValueError:
            self._failure("Invalid IP address format", value=value)


class HostPortPair(t.Trafaret):
    def __init__(self, *, allow_blank_host: bool = False) -> None:
        super().__init__()
        self._allow_blank_host = allow_blank_host

    def check_and_return(self, value: Any) -> Tuple[ipaddress._BaseAddress, int]:
        host: str | ipaddress._BaseAddress
        if isinstance(value, str):
            pair = value.rsplit(":", maxsplit=1)
            if len(pair) == 1:
                self._failure("value as string must contain both address and number", value=value)
            host, port = pair[0], pair[1]
        elif isinstance(value, Sequence):
            if len(value) != 2:
                self._failure(
                    "value as array must contain only two values for address and number",
                    value=value,
                )
            host, port = value[0], value[1]
        elif isinstance(value, Mapping):
            try:
                host, port = value["host"], value["port"]
            except KeyError:
                self._failure('value as map must contain "host" and "port" keys', value=value)
        else:
            self._failure("urecognized value type", value=value)
        try:
            if isinstance(host, str):
                host = ipaddress.ip_address(host.strip("[]"))
            elif isinstance(host, ipaddress._BaseAddress):
                pass
        except ValueError:
            pass
        if not self._allow_blank_host and not host:
            self._failure("value has empty host", value=value)
        try:
            port = t.ToInt[1:65535].check(port)
        except t.DataError:
            self._failure("port number must be between 1 and 65535", value=value)
        return _HostPortPair(host, port)