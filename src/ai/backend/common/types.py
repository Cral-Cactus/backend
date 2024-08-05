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

class BinarySize(int):

    suffix_map = {
        "y": 2**80,
        "Y": 2**80,  # yotta
        "z": 2**70,
        "Z": 2**70,  # zetta
        "e": 2**60,
        "E": 2**60,  # exa
        "p": 2**50,
        "P": 2**50,  # peta
        "t": 2**40,
        "T": 2**40,  # tera
        "g": 2**30,
        "G": 2**30,  # giga
        "m": 2**20,
        "M": 2**20,  # mega
        "k": 2**10,
        "K": 2**10,  # kilo
        " ": 1,
    }
    suffices = (" ", "K", "M", "G", "T", "P", "E", "Z", "Y")
    endings = ("ibytes", "ibyte", "ib", "bytes", "byte", "b")

    @classmethod
    def _parse_str(cls, expr: str) -> Union[BinarySize, Decimal]:
        if expr.lower() in ("inf", "infinite", "infinity"):
            return Decimal("Infinity")
        orig_expr = expr
        expr = expr.strip().replace("_", "")
        try:
            return cls(expr)
        except ValueError:
            expr = expr.lower()
            dec_expr: Decimal
            try:
                for ending in cls.endings:
                    if expr.endswith(ending):
                        length = len(ending) + 1
                        suffix = expr[-length]
                        dec_expr = Decimal(expr[:-length])
                        break
                else:
                    if not str.isnumeric(expr[-1]):
                        suffix = expr[-1]
                        dec_expr = Decimal(expr[:-1])
                    else:
                        raise ValueError("Fractional bytes are not allowed")
            except ArithmeticError:
                raise ValueError("Unconvertible value", orig_expr)
            try:
                multiplier = cls.suffix_map[suffix]
            except KeyError:
                raise ValueError("Unconvertible value", orig_expr)
            return cls(dec_expr * multiplier)

    @classmethod
    def finite_from_str(
        cls,
        expr: Union[str, Decimal, numbers.Integral],
    ) -> BinarySize:
        if isinstance(expr, Decimal):
            if expr.is_infinite():
                raise ValueError("infinite values are not allowed")
            return cls(expr)
        if isinstance(expr, numbers.Integral):
            return cls(int(expr))
        result = cls._parse_str(expr)
        if isinstance(result, Decimal) and result.is_infinite():
            raise ValueError("infinite values are not allowed")
        return cls(int(result))

    @classmethod
    def from_str(
        cls,
        expr: Union[str, Decimal, numbers.Integral],
    ) -> Union[BinarySize, Decimal]:
        if isinstance(expr, Decimal):
            return cls(expr)
        if isinstance(expr, numbers.Integral):
            return cls(int(expr))
        return cls._parse_str(expr)

    def _preformat(self):
        scale = self
        suffix_idx = 0
        while scale >= 1024:
            scale //= 1024
            suffix_idx += 1
        return suffix_idx

    @staticmethod
    def _quantize(val, multiplier):
        d = Decimal(val) / Decimal(multiplier)
        if d == d.to_integral():
            value = d.quantize(Decimal(1))
        else:
            value = d.quantize(Decimal(".00")).normalize()
        return value

    def __str__(self):
        suffix_idx = self._preformat()
        if suffix_idx == 0:
            if self == 1:
                return f"{int(self)} byte"
            else:
                return f"{int(self)} bytes"
        else:
            suffix = type(self).suffices[suffix_idx]
            multiplier = type(self).suffix_map[suffix]
            value = self._quantize(self, multiplier)
            return f"{value} {suffix.upper()}iB"

    def __format__(self, format_spec):
        if len(format_spec) != 1:
            raise ValueError("format-string for BinarySize can be only one character.")
        if format_spec == "s":
            suffix_idx = self._preformat()
            if suffix_idx == 0:
                return f"{int(self)}"
            suffix = type(self).suffices[suffix_idx]
            multiplier = type(self).suffix_map[suffix]
            value = self._quantize(self, multiplier)
            return f"{value}{suffix.lower()}"
        else:
            suffix = format_spec.lower()
            multiplier = type(self).suffix_map.get(suffix)
            if multiplier is None:
                raise ValueError("Unsupported scale unit.", suffix)
            value = self._quantize(self, multiplier)
            return f"{value}{suffix.lower()}".strip()


class ResourceSlot(UserDict):
    __slots__ = ("data",)

    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)

    def sync_keys(self, other: ResourceSlot) -> None:
        self_only_keys = self.data.keys() - other.data.keys()
        other_only_keys = other.data.keys() - self.data.keys()
        for k in self_only_keys:
            other.data[k] = Decimal(0)
        for k in other_only_keys:
            self.data[k] = Decimal(0)

    def __add__(self, other: ResourceSlot) -> ResourceSlot:
        assert isinstance(other, ResourceSlot), "Only can add ResourceSlot to ResourceSlot."
        self.sync_keys(other)
        return type(self)({
            k: self.get(k, 0) + other.get(k, 0) for k in (self.keys() | other.keys())
        })

    def __sub__(self, other: ResourceSlot) -> ResourceSlot:
        assert isinstance(other, ResourceSlot), "Only can subtract ResourceSlot from ResourceSlot."
        self.sync_keys(other)
        return type(self)({k: self.data[k] - other.get(k, 0) for k in self.keys()})

    def __neg__(self):
        return type(self)({k: -v for k, v in self.data.items()})

    def __eq__(self, other: object) -> bool:
        if other is self:
            return True
        assert isinstance(other, ResourceSlot), "Only can compare ResourceSlot objects."
        self.sync_keys(other)
        self_values = [self.data[k] for k in sorted(self.data.keys())]
        other_values = [other.data[k] for k in sorted(other.data.keys())]
        return self_values == other_values

    def __ne__(self, other: object) -> bool:
        assert isinstance(other, ResourceSlot), "Only can compare ResourceSlot objects."
        self.sync_keys(other)
        return not self.__eq__(other)

    def eq_contains(self, other: ResourceSlot) -> bool:
        assert isinstance(other, ResourceSlot), "Only can compare ResourceSlot objects."
        common_keys = sorted(other.keys() & self.keys())
        only_other_keys = other.keys() - self.keys()
        self_values = [self.data[k] for k in common_keys]
        other_values = [other.data[k] for k in common_keys]
        return self_values == other_values and all(other[k] == 0 for k in only_other_keys)

    def eq_contained(self, other: ResourceSlot) -> bool:
        assert isinstance(other, ResourceSlot), "Only can compare ResourceSlot objects."
        common_keys = sorted(other.keys() & self.keys())
        only_self_keys = self.keys() - other.keys()
        self_values = [self.data[k] for k in common_keys]
        other_values = [other.data[k] for k in common_keys]
        return self_values == other_values and all(self[k] == 0 for k in only_self_keys)

    def __le__(self, other: ResourceSlot) -> bool:
        assert isinstance(other, ResourceSlot), "Only can compare ResourceSlot objects."
        self.sync_keys(other)
        self_values = [self.data[k] for k in self.keys()]
        other_values = [other.data[k] for k in self.keys()]
        return not any(s > o for s, o in zip(self_values, other_values))

    def __lt__(self, other: ResourceSlot) -> bool:
        assert isinstance(other, ResourceSlot), "Only can compare ResourceSlot objects."
        self.sync_keys(other)
        self_values = [self.data[k] for k in self.keys()]
        other_values = [other.data[k] for k in self.keys()]
        return not any(s > o for s, o in zip(self_values, other_values)) and not (
            self_values == other_values
        )

    def __ge__(self, other: ResourceSlot) -> bool:
        assert isinstance(other, ResourceSlot), "Only can compare ResourceSlot objects."
        self.sync_keys(other)
        self_values = [self.data[k] for k in other.keys()]
        other_values = [other.data[k] for k in other.keys()]
        return not any(s < o for s, o in zip(self_values, other_values))