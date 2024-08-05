from __future__ import annotations

import dataclasses
import enum
import ipaddress
import itertools
import math
import numbers
import sys
import uuid
from abc import ABCMeta, abstractmethod
from collections import UserDict, defaultdict, namedtuple
from contextvars import ContextVar
from dataclasses import dataclass
from decimal import Decimal
from ipaddress import ip_address, ip_network
from pathlib import Path, PurePosixPath
from ssl import SSLContext
from typing import (
    TYPE_CHECKING,
    Any,
    Dict,
    Generic,
    List,
    Literal,
    Mapping,
    NewType,
    NotRequired,
    Optional,
    Sequence,
    Tuple,
    Type,
    TypeAlias,
    TypedDict,
    TypeVar,
    Union,
    cast,
    overload,
)

import attrs
import redis.asyncio.sentinel
import trafaret as t
import typeguard
from aiohttp import Fingerprint
from pydantic import BaseModel, ConfigDict, Field
from redis.asyncio import Redis

from .exception import InvalidIpAddressValue
from .models.minilang.mount import MountPointParser

__all__ = (
    "aobject",
    "JSONSerializableMixin",
    "DeviceId",
    "ContainerId",
    "EndpointId",
    "SessionId",
    "KernelId",
    "MetricKey",
    "MetricValue",
    "MovingStatValue",
)

if TYPE_CHECKING:
    from .docker import ImageRef


T_aobj = TypeVar("T_aobj", bound="aobject")

current_resource_slots: ContextVar[Mapping[SlotName, SlotTypes]] = ContextVar(
    "current_resource_slots"
)


class aobject(object):

    @classmethod
    async def new(cls: Type[T_aobj], *args, **kwargs) -> T_aobj:
        instance = super().__new__(cls)
        cls.__init__(instance, *args, **kwargs)
        await instance.__ainit__()
        return instance

    def __init__(self, *args, **kwargs) -> None:
        pass

    async def __ainit__(self) -> None:
        pass


T1 = TypeVar("T1")
T2 = TypeVar("T2")
T3 = TypeVar("T3")
T4 = TypeVar("T4")


@overload
def check_typed_tuple(
    value: Tuple[Any],
    types: Tuple[Type[T1]],
) -> Tuple[T1]: ...


@overload
def check_typed_tuple(
    value: Tuple[Any, Any],
    types: Tuple[Type[T1], Type[T2]],
) -> Tuple[T1, T2]: ...


@overload
def check_typed_tuple(
    value: Tuple[Any, Any, Any],
    types: Tuple[Type[T1], Type[T2], Type[T3]],
) -> Tuple[T1, T2, T3]: ...


@overload
def check_typed_tuple(
    value: Tuple[Any, Any, Any, Any],
    types: Tuple[Type[T1], Type[T2], Type[T3], Type[T4]],
) -> Tuple[T1, T2, T3, T4]: ...


def check_typed_tuple(value: Tuple[Any, ...], types: Tuple[Type, ...]) -> Tuple:
    for val, typ in itertools.zip_longest(value, types):
        if typ is not None:
            typeguard.check_type("item", val, typ)
    return value


TD = TypeVar("TD")


def check_typed_dict(value: Mapping[Any, Any], expected_type: Type[TD]) -> TD:
    assert issubclass(expected_type, dict) and hasattr(
        expected_type, "__annotations__"
    ), f"expected_type ({type(expected_type)}) must be a TypedDict class"
    frame = sys._getframe(1)
    _globals = frame.f_globals
    _locals = frame.f_locals
    memo = typeguard._TypeCheckMemo(_globals, _locals)
    typeguard.check_typed_dict("value", value, expected_type, memo)
    return cast(TD, value)

    class MountExpression:
    def __init__(self, expression: str, *, escape_map: Optional[Mapping[str, str]] = None) -> None:
        self.expression = expression
        self.escape_map = {
            "\\,": ",",
            "\\:": ":",
            "\\=": "=",
        }
        if escape_map is not None:
            self.escape_map.update(escape_map)

    def __str__(self) -> str:
        return self.expression

    def __repr__(self) -> str:
        return self.__str__()

    def parse(self, *, escape: bool = True) -> Mapping[str, str]:
        parser = MountPointParser()
        result = {**parser.parse_mount(self.expression)}
        if escape:
            for key, value in result.items():
                for raw, alternative in self.escape_map.items():
                    if raw in value:
                        result[key] = value.replace(raw, alternative)
        return MountPoint(**result).model_dump()


class HostPortPair(namedtuple("HostPortPair", "host port")):
    def as_sockaddr(self) -> Tuple[str, int]:
        return str(self.host), self.port

    def __str__(self) -> str:
        if isinstance(self.host, ipaddress.IPv6Address):
            return f"[{self.host}]:{self.port}"
        return f"{self.host}:{self.port}"


_Address = TypeVar("_Address", bound=Union[ipaddress.IPv4Network, ipaddress.IPv6Network])


class ReadableCIDR(Generic[_Address]):

    _address: _Address | None

    def __init__(self, address: str | None, is_network: bool = True) -> None:
        self._is_network = is_network
        self._address = self._convert_to_cidr(address) if address is not None else None

    def _convert_to_cidr(self, value: str) -> _Address:
        str_val = str(value)
        if not self._is_network:
            return cast(_Address, ip_address(str_val))
        if "*" in str_val:
            _ip, _, given_cidr = str_val.partition("/")
            filtered = _ip.replace("*", "0")
            if given_cidr:
                return self._to_ip_network(f"{filtered}/{given_cidr}")
            octets = _ip.split(".")
            cidr = octets.index("*") * 8
            return self._to_ip_network(f"{filtered}/{cidr}")
        return self._to_ip_network(str_val)

    @staticmethod
    def _to_ip_network(val: str) -> _Address:
        try:
            return cast(_Address, ip_network(val))
        except ValueError:
            raise InvalidIpAddressValue

    @property
    def address(self) -> _Address | None:
        return self._address

    def __str__(self) -> str:
        return str(self._address)

    def __eq__(self, other: object) -> bool:
        if other is self:
            return True
        assert isinstance(other, ReadableCIDR), "Only can compare ReadableCIDR objects."
        return self.address == other.address