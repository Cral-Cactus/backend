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

            def __gt__(self, other: ResourceSlot) -> bool:
        assert isinstance(other, ResourceSlot), "Only can compare ResourceSlot objects."
        self.sync_keys(other)
        self_values = [self.data[k] for k in other.keys()]
        other_values = [other.data[k] for k in other.keys()]
        return not any(s < o for s, o in zip(self_values, other_values)) and not (
            self_values == other_values
        )

    def normalize_slots(self, *, ignore_unknown: bool) -> ResourceSlot:
        known_slots = current_resource_slots.get()
        unset_slots = known_slots.keys() - self.data.keys()
        if not ignore_unknown and (unknown_slots := self.data.keys() - known_slots.keys()):
            raise ValueError(f"Unknown slots: {', '.join(map(repr, unknown_slots))}")
        data = {k: v for k, v in self.data.items() if k in known_slots}
        for k in unset_slots:
            data[k] = Decimal(0)
        return type(self)(data)

    @classmethod
    def _normalize_value(cls, key: str, value: Any, unit: SlotTypes) -> Decimal:
        try:
            if unit == SlotTypes.BYTES:
                if isinstance(value, Decimal):
                    return Decimal(value) if value.is_finite() else value
                if isinstance(value, int):
                    return Decimal(value)
                value = Decimal(BinarySize.from_str(value))
            else:
                value = Decimal(value)
                if value.is_finite():
                    value = value.quantize(Quantum).normalize()
        except (
            ArithmeticError,
            ValueError,
        ):
            raise ValueError(f"Cannot convert the slot {key!r} to decimal: {value!r}")
        return value

    @classmethod
    def _humanize_value(cls, value: Decimal, unit: str) -> str:
        if unit == "bytes":
            try:
                result = "{:s}".format(BinarySize(value))
            except (OverflowError, ValueError):
                result = _stringify_number(value)
        else:
            result = _stringify_number(value)
        return result

    @classmethod
    def _guess_slot_type(cls, key: str) -> SlotTypes:
        if "mem" in key:
            return SlotTypes.BYTES
        return SlotTypes.COUNT

    @classmethod
    def from_policy(cls, policy: Mapping[str, Any], slot_types: Mapping) -> "ResourceSlot":
        try:
            data = {
                k: cls._normalize_value(k, v, slot_types[k])
                for k, v in policy["total_resource_slots"].items()
                if v is not None and k in slot_types
            }
            fill = Decimal(0)
            if policy["default_for_unspecified"] == DefaultForUnspecified.UNLIMITED:
                fill = Decimal("Infinity")
            for k in slot_types.keys():
                if k not in data:
                    data[k] = fill
        except KeyError as e:
            raise ValueError(f"Unknown slot type: {e.args[0]!r}")
        return cls(data)

    @classmethod
    def from_user_input(
        cls,
        obj: Mapping[str, Any],
        slot_types: Optional[Mapping[SlotName, SlotTypes]],
    ) -> "ResourceSlot":
        try:
            if slot_types is None:
                data = {
                    k: cls._normalize_value(k, v, cls._guess_slot_type(k))
                    for k, v in obj.items()
                    if v is not None
                }
            else:
                data = {
                    k: cls._normalize_value(k, v, slot_types[SlotName(k)])
                    for k, v in obj.items()
                    if v is not None
                }
                # fill missing
                for k in slot_types.keys():
                    if k not in data:
                        data[k] = Decimal(0)
        except KeyError as e:
            extra_guide = ""
            if e.args[0] == "shmem":
                extra_guide = " (Put it at the 'resource_opts' field in API, or use '--resource-opts shmem=...' in CLI)"
            raise ValueError(f"Unknown slot type: {e.args[0]!r}" + extra_guide)
        return cls(data)

    def to_humanized(self, slot_types: Mapping) -> Mapping[str, str]:
        try:
            return {
                k: type(self)._humanize_value(v, slot_types[k])
                for k, v in self.data.items()
                if v is not None
            }
        except KeyError as e:
            raise ValueError(f"Unknown slot type: {e.args[0]!r}")

    @classmethod
    def from_json(cls, obj: Mapping[str, Any]) -> "ResourceSlot":
        data = {k: Decimal(v) for k, v in obj.items() if v is not None}
        return cls(data)

    def to_json(self) -> Mapping[str, str]:
        return {k: _stringify_number(Decimal(v)) for k, v in self.data.items() if v is not None}


class JSONSerializableMixin(metaclass=ABCMeta):
    @abstractmethod
    def to_json(self) -> dict[str, Any]:
        raise NotImplementedError

    @classmethod
    def from_json(cls, obj: Mapping[str, Any]) -> JSONSerializableMixin:
        return cls(**cls.as_trafaret().check(obj))

    @classmethod
    @abstractmethod
    def as_trafaret(cls) -> t.Trafaret:
        raise NotImplementedError


@attrs.define(slots=True, frozen=True)
class QuotaScopeID:
    scope_type: QuotaScopeType
    scope_id: uuid.UUID

    @classmethod
    def parse(cls, raw: str) -> QuotaScopeID:
        scope_type, _, rest = raw.partition(":")
        match scope_type.lower():
            case QuotaScopeType.PROJECT | QuotaScopeType.USER as t:
                return cls(t, uuid.UUID(rest))
            case _:
                raise ValueError(f"Invalid quota scope type: {scope_type!r}")

    def __str__(self) -> str:
        match self.scope_id:
            case uuid.UUID():
                return f"{self.scope_type}:{str(self.scope_id)}"
            case _:
                raise ValueError(f"Invalid quota scope ID: {self.scope_id!r}")

    def __repr__(self) -> str:
        return self.__str__()

    @property
    def pathname(self) -> str:
        match self.scope_id:
            case uuid.UUID():
                return self.scope_id.hex
            case _:
                raise ValueError(f"Invalid quota scope ID: {self.scope_id!r}")


class VFolderID:
    quota_scope_id: QuotaScopeID | None
    folder_id: uuid.UUID

    @classmethod
    def from_row(cls, row: Any) -> VFolderID:
        return VFolderID(quota_scope_id=row["quota_scope_id"], folder_id=row["id"])

    def __init__(self, quota_scope_id: QuotaScopeID | str | None, folder_id: uuid.UUID) -> None:
        self.folder_id = folder_id
        match quota_scope_id:
            case QuotaScopeID():
                self.quota_scope_id = quota_scope_id
            case str():
                self.quota_scope_id = QuotaScopeID.parse(quota_scope_id)
            case None:
                self.quota_scope_id = None
            case _:
                self.quota_scope_id = QuotaScopeID.parse(str(quota_scope_id))

    def __str__(self) -> str:
        if self.quota_scope_id is None:
            return self.folder_id.hex
        return f"{self.quota_scope_id}/{self.folder_id.hex}"

    def __eq__(self, other) -> bool:
        return self.quota_scope_id == other.quota_scope_id and self.folder_id == other.folder_id


    @classmethod
    def as_trafaret(cls) -> t.Trafaret:
        from . import validators as tx

        return t.Dict({
            t.Key("name"): t.String,
            t.Key("vfid"): tx.VFolderID,
            t.Key("vfsubpath", default="."): tx.PurePath,
            t.Key("host_path"): tx.PurePath,
            t.Key("kernel_path"): tx.PurePath,
            t.Key("mount_perm"): tx.Enum(MountPermission),
            t.Key("usage_mode", default=VFolderUsageMode.GENERAL): t.Null
            | tx.Enum(VFolderUsageMode),
        })


class VFolderHostPermissionMap(dict, JSONSerializableMixin):
    def __or__(self, other: Any) -> VFolderHostPermissionMap:
        if self is other:
            return self
        if not isinstance(other, dict):
            raise ValueError(f"Invalid type. expected `dict` type, got {type(other)} type")
        union_map: Dict[str, set] = defaultdict(set)
        for host, perms in [*self.items(), *other.items()]:
            try:
                perm_list = [VFolderHostPermission(perm) for perm in perms]
            except ValueError:
                raise ValueError(f"Invalid type. Permissions of Host `{host}` are ({perms})")
            union_map[host] |= set(perm_list)
        return VFolderHostPermissionMap(union_map)

    def to_json(self) -> dict[str, Any]:
        return {host: [perm.value for perm in perms] for host, perms in self.items()}

    @classmethod
    def from_json(cls, obj: Mapping[str, Any]) -> JSONSerializableMixin:
        return cls(**cls.as_trafaret().check(obj))

    @classmethod
    def as_trafaret(cls) -> t.Trafaret:
        from . import validators as tx

        return t.Dict(t.String, t.List(tx.Enum(VFolderHostPermission)))


@attrs.define(auto_attribs=True, slots=True)
class QuotaConfig:
    limit_bytes: int

    class Validator(t.Trafaret):
        def check_and_return(self, value: Any) -> QuotaConfig:
            validator = t.Dict({
                t.Key("limit_bytes"): t.ToInt(),
            })
            converted = validator.check(value)
            return QuotaConfig(
                limit_bytes=converted["limit_bytes"],
            )

    @classmethod
    def as_trafaret(cls) -> t.Trafaret:
        return cls.Validator()


class QuotaScopeType(enum.StrEnum):
    USER = "user"
    PROJECT = "project"


class ImageRegistry(TypedDict):
    name: str
    url: str
    username: Optional[str]
    password: Optional[str]


class ImageConfig(TypedDict):
    canonical: str
    architecture: str
    digest: str
    repo_digest: Optional[str]
    registry: ImageRegistry
    labels: Mapping[str, str]
    is_local: bool


class ServicePort(TypedDict):
    name: str
    protocol: ServicePortProtocols
    container_ports: Sequence[int]
    host_ports: Sequence[Optional[int]]
    is_inference: bool


ClusterSSHPortMapping = NewType("ClusterSSHPortMapping", Mapping[str, Tuple[str, int]])


class ClusterInfo(TypedDict):
    mode: ClusterMode
    size: int
    replicas: Mapping[str, int]
    network_name: Optional[str]
    ssh_keypair: ClusterSSHKeyPair
    cluster_ssh_port_mapping: Optional[ClusterSSHPortMapping]


class ClusterSSHKeyPair(TypedDict):
    public_key: str 
    private_key: str

class DeviceModelInfo(TypedDict):
    device_id: DeviceId | str
    model_name: str
    data: Mapping[str, Any]


class KernelCreationResult(TypedDict):
    id: KernelId
    container_id: ContainerId
    service_ports: Sequence[ServicePort]
    kernel_host: str
    resource_spec: Mapping[str, Any]
    attached_devices: Mapping[DeviceName, Sequence[DeviceModelInfo]]
    repl_in_port: int
    repl_out_port: int
    stdin_port: int
    stdout_port: int
    scaling_group: str
    agent_addr: str


class KernelCreationConfig(TypedDict):
    image: ImageConfig
    auto_pull: AutoPullBehavior
    session_type: SessionTypes
    cluster_mode: ClusterMode
    cluster_role: str
    cluster_idx: int
    cluster_hostname: str
    resource_slots: Mapping[str, str]
    resource_opts: Mapping[str, str]
    environ: Mapping[str, str]
    mounts: Sequence[Mapping[str, Any]]
    package_directory: Sequence[str]
    idle_timeout: int
    bootstrap_script: Optional[str]
    startup_command: Optional[str]
    internal_data: Optional[Mapping[str, Any]]
    preopen_ports: List[int]
    allocated_host_ports: List[int]
    scaling_group: str
    agent_addr: str
    endpoint_id: Optional[str]


class SessionEnqueueingConfig(TypedDict):
    creation_config: dict
    kernel_configs: List[KernelEnqueueingConfig]


class KernelEnqueueingConfig(TypedDict):
    image_ref: ImageRef
    cluster_role: str
    cluster_idx: int
    local_rank: int
    cluster_hostname: str
    creation_config: dict
    bootstrap_script: str
    startup_command: Optional[str]


def _stringify_number(v: Union[BinarySize, int, float, Decimal]) -> str:
    if isinstance(v, (float, Decimal)):
        if math.isinf(v) and v > 0:
            result = "Infinity"
        elif math.isinf(v) and v < 0:
            result = "-Infinity"
        else:
            result = "{:f}".format(v)
    elif isinstance(v, BinarySize):
        result = "{:d}".format(int(v))
    elif isinstance(v, int):
        result = "{:d}".format(v)
    else:
        result = str(v)
    return result


class Sentinel(enum.Enum):
    TOKEN = 0


class QueueSentinel(enum.Enum):
    CLOSED = 0
    TIMEOUT = 1

class AcceleratorMetadata(TypedDict):
    slot_name: str
    description: str
    human_readable_name: str
    display_unit: str
    number_format: AcceleratorNumberFormat
    display_icon: str


class AgentSelectionStrategy(enum.StrEnum):
    DISPERSED = "dispersed"
    CONCENTRATED = "concentrated"
    LEGACY = "legacy"


class SchedulerStatus(TypedDict):
    trigger_event: str
    execution_time: str
    finish_time: NotRequired[str]
    resource_group: NotRequired[str]
    endpoint_name: NotRequired[str]
    action: NotRequired[str]


class VolumeMountableNodeType(enum.StrEnum):
    AGENT = enum.auto()
    STORAGE_PROXY = enum.auto()


@dataclass
class RoundRobinState(JSONSerializableMixin):
    schedulable_group_id: str
    next_index: int

    def to_json(self) -> dict[str, Any]:
        return dataclasses.asdict(self)

    @classmethod
    def from_json(cls, obj: Mapping[str, Any]) -> RoundRobinState:
        return cls(**cls.as_trafaret().check(obj))

    @classmethod
    def as_trafaret(cls) -> t.Trafaret:
        return t.Dict({
            t.Key("schedulable_group_id"): t.String,
            t.Key("next_index"): t.Int,
        })


RoundRobinStates: TypeAlias = dict[str, dict[str, RoundRobinState]]

SSLContextType: TypeAlias = bool | Fingerprint | SSLContext


class ModelServiceStatus(enum.Enum):
    HEALTHY = "healthy"
    UNHEALTHY = "unhealthy"


class RuntimeVariant(enum.StrEnum):
    VLLM = "vllm"
    NIM = "nim"
    CMD = "cmd"
    CUSTOM = "custom"


@dataclass
class ModelServiceProfile:
    name: str
    health_check_endpoint: str | None = dataclasses.field(default=None)
    port: int | None = dataclasses.field(default=None)


MODEL_SERVICE_RUNTIME_PROFILES: Mapping[RuntimeVariant, ModelServiceProfile] = {
    RuntimeVariant.CUSTOM: ModelServiceProfile(name="Custom (Default)"),
    RuntimeVariant.VLLM: ModelServiceProfile(
        name="vLLM", health_check_endpoint="/health", port=8000
    ),
    RuntimeVariant.NIM: ModelServiceProfile(
        name="NVIDIA NIM", health_check_endpoint="/v1/health/ready", port=8000
    ),
    RuntimeVariant.CMD: ModelServiceProfile(name="Predefined Image Command"),
}