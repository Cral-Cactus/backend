from __future__ import annotations

import asyncio
import enum
import functools
import logging
from collections import ChainMap, namedtuple
from typing import (
    AsyncGenerator,
    Callable,
    Iterable,
    List,
    Mapping,
    MutableMapping,
    Optional,
    ParamSpec,
    Tuple,
    TypeAlias,
    TypeVar,
    Union,
    cast,
)
from urllib.parse import quote as _quote
from urllib.parse import unquote

import trafaret as t
from etcd_client import (
    Client as EtcdClient,
)
from etcd_client import (
    Communicator as EtcdCommunicator,
)
from etcd_client import (
    Compare,
    CompareOp,
    CondVar,
    ConnectOptions,
    GRPCStatusCode,
    GRPCStatusError,
    TxnOp,
    Watch,
)
from etcd_client import (
    Txn as EtcdTransactionAction,
)

from .logging_utils import BraceStyleAdapter
from .types import HostPortPair, QueueSentinel

__all__ = (
    "quote",
    "unquote",
    "AsyncEtcd",
)

Event = namedtuple("Event", "key event value")

log = BraceStyleAdapter(logging.getLogger(__spec__.name))

class ConfigScopes(enum.Enum):
    MERGED = 0
    GLOBAL = 1
    SGROUP = 2
    NODE = 3


quote = functools.partial(_quote, safe="")


def make_dict_from_pairs(key_prefix, pairs, path_sep="/"):
    result = {}
    len_prefix = len(key_prefix)
    if isinstance(pairs, dict):
        iterator = pairs.items()
    else:
        iterator = pairs
    for k, v in iterator:
        if not k.startswith(key_prefix):
            continue
        subkey = k[len_prefix:]
        if subkey.startswith(path_sep):
            subkey = subkey[1:]
        path_components = subkey.split("/")
        parent = result
        for p in path_components[:-1]:
            p = unquote(p)
            if p not in parent:
                parent[p] = {}
            if p in parent and not isinstance(parent[p], dict):
                root = parent[p]
                parent[p] = {"": root}
            parent = parent[p]
        parent[unquote(path_components[-1])] = v
    return result


def _slash(v: str):
    return v.rstrip("/") + "/" if len(v) > 0 else ""


P = ParamSpec("P")
R = TypeVar("R")

GetPrefixValue: TypeAlias = "Mapping[str, GetPrefixValue | Optional[str]]"
NestedStrKeyedMapping: TypeAlias = "Mapping[str, str | NestedStrKeyedMapping]"
NestedStrKeyedDict: TypeAlias = "dict[str, str | NestedStrKeyedDict]"


class AsyncEtcd:
    etcd: EtcdClient
    _connect_options: Optional[ConnectOptions]

    def __init__(
        self,
        addr: HostPortPair,
        namespace: str,
        scope_prefix_map: Mapping[ConfigScopes, str],
        *,
        credentials: dict[str, str] | None = None,
        encoding: str = "utf-8",
        watch_reconnect_intvl: float = 0.5,
    ) -> None:
        self.scope_prefix_map = t.Dict({
            t.Key(ConfigScopes.GLOBAL): t.String(allow_blank=True),
            t.Key(ConfigScopes.SGROUP, optional=True): t.String,
            t.Key(ConfigScopes.NODE, optional=True): t.String,
        }).check(scope_prefix_map)

        if credentials is not None:
            self._connect_options = ConnectOptions().with_user(
                credentials["user"], credentials["password"]
            )
        else:
            self._connect_options = None

        self.ns = namespace
        log.info('using etcd cluster from {} with namespace "{}"', addr, namespace)
        self.encoding = encoding
        self.watch_reconnect_intvl = watch_reconnect_intvl

        self.etcd = EtcdClient(
            [f"http://{addr.host}:{addr.port}"],
            connect_options=self._connect_options,
        )


        def _demangle_key(self, k: Union[bytes, str]) -> str:
        if isinstance(k, bytes):
            k = k.decode(self.encoding)
        prefix = f"/sorna/{self.ns}/"
        if k.startswith(prefix):
            k = k[len(prefix) :]
        return k

    def _merge_scope_prefix_map(
        self,
        override: Mapping[ConfigScopes, str] = None,
    ) -> Mapping[ConfigScopes, str]:

        return ChainMap(cast(MutableMapping, override) or {}, self.scope_prefix_map)

    async def put(
        self,
        key: str,
        val: str,
        *,
        scope: ConfigScopes = ConfigScopes.GLOBAL,
        scope_prefix_map: Mapping[ConfigScopes, str] = None,
    ):

        scope_prefix = self._merge_scope_prefix_map(scope_prefix_map)[scope]
        mangled_key = self._mangle_key(f"{_slash(scope_prefix)}{key}")
        async with self.etcd.connect() as communicator:
            await communicator.put(
                mangled_key.encode(self.encoding), str(val).encode(self.encoding)
            )

    async def put_prefix(
        self,
        key: str,
        dict_obj: NestedStrKeyedMapping,
        *,
        scope: ConfigScopes = ConfigScopes.GLOBAL,
        scope_prefix_map: Mapping[ConfigScopes, str] = None,
    ):

        scope_prefix = self._merge_scope_prefix_map(scope_prefix_map)[scope]
        flattened_dict: NestedStrKeyedDict = {}

        def _flatten(prefix: str, inner_dict: NestedStrKeyedDict) -> None:
            for k, v in inner_dict.items():
                if k == "":
                    flattened_key = prefix
                else:
                    flattened_key = prefix + "/" + quote(k)
                if isinstance(v, dict):
                    _flatten(flattened_key, v)
                else:
                    flattened_dict[flattened_key] = v

        _flatten(key, cast(NestedStrKeyedDict, dict_obj))

        actions = []
        for k, v in flattened_dict.items():
            actions.append(
                TxnOp.put(
                    self._mangle_key(f"{_slash(scope_prefix)}{k}").encode(self.encoding),
                    str(v).encode(self.encoding),
                )
            )

        async with self.etcd.connect() as communicator:
            await communicator.txn(EtcdTransactionAction().and_then(actions).or_else([]))

    async def put_dict(
        self,
        flattened_dict_obj: Mapping[str, str],
        *,
        scope: ConfigScopes = ConfigScopes.GLOBAL,
        scope_prefix_map: Mapping[ConfigScopes, str] = None,
    ):

        scope_prefix = self._merge_scope_prefix_map(scope_prefix_map)[scope]

        actions = []
        for k, v in flattened_dict_obj.items():
            actions.append(
                TxnOp.put(
                    self._mangle_key(f"{_slash(scope_prefix)}{k}").encode(self.encoding),
                    str(v).encode(self.encoding),
                )
            )

        async with self.etcd.connect() as communicator:
            await communicator.txn(EtcdTransactionAction().and_then(actions).or_else([]))

    async def get(
        self,
        key: str,
        *,
        scope: ConfigScopes = ConfigScopes.MERGED,
        scope_prefix_map: Mapping[ConfigScopes, str] = None,
    ) -> Optional[str]:

        _scope_prefix_map = self._merge_scope_prefix_map(scope_prefix_map)
        if scope == ConfigScopes.MERGED or scope == ConfigScopes.NODE:
            scope_prefixes = [_scope_prefix_map[ConfigScopes.GLOBAL]]
            p = _scope_prefix_map.get(ConfigScopes.SGROUP)
            if p is not None:
                scope_prefixes.insert(0, p)
            p = _scope_prefix_map.get(ConfigScopes.NODE)
            if p is not None:
                scope_prefixes.insert(0, p)
        elif scope == ConfigScopes.SGROUP:
            scope_prefixes = [_scope_prefix_map[ConfigScopes.GLOBAL]]
            p = _scope_prefix_map.get(ConfigScopes.SGROUP)
            if p is not None:
                scope_prefixes.insert(0, p)
        elif scope == ConfigScopes.GLOBAL:
            scope_prefixes = [_scope_prefix_map[ConfigScopes.GLOBAL]]
        else:
            raise ValueError("Invalid scope prefix value")

        async with self.etcd.connect() as communicator:
            for scope_prefix in scope_prefixes:
                value = await communicator.get(
                    self._mangle_key(f"{_slash(scope_prefix)}{key}").encode(self.encoding)
                )
                if value is not None:
                    return bytes(value).decode(self.encoding)
        return None

    async def get_prefix(
        self,
        key_prefix: str,
        *,
        scope: ConfigScopes = ConfigScopes.MERGED,
        scope_prefix_map: Mapping[ConfigScopes, str] = None,
    ) -> GetPrefixValue:

        _scope_prefix_map = self._merge_scope_prefix_map(scope_prefix_map)
        if scope == ConfigScopes.MERGED or scope == ConfigScopes.NODE:
            scope_prefixes = [_scope_prefix_map[ConfigScopes.GLOBAL]]
            p = _scope_prefix_map.get(ConfigScopes.SGROUP)
            if p is not None:
                scope_prefixes.insert(0, p)
            p = _scope_prefix_map.get(ConfigScopes.NODE)
            if p is not None:
                scope_prefixes.insert(0, p)
                        elif scope == ConfigScopes.SGROUP:
            scope_prefixes = [_scope_prefix_map[ConfigScopes.GLOBAL]]
            p = _scope_prefix_map.get(ConfigScopes.SGROUP)
            if p is not None:
                scope_prefixes.insert(0, p)
        elif scope == ConfigScopes.GLOBAL:
            scope_prefixes = [_scope_prefix_map[ConfigScopes.GLOBAL]]
        else:
            raise ValueError("Invalid scope prefix value")
        pair_sets: List[List[Mapping | Tuple]] = []
        async with self.etcd.connect() as communicator:
            for scope_prefix in scope_prefixes:
                mangled_key_prefix = self._mangle_key(f"{_slash(scope_prefix)}{key_prefix}")
                values = await communicator.get_prefix(mangled_key_prefix.encode(self.encoding))
                pair_sets.append([
                    (
                        self._demangle_key(bytes(k).decode(self.encoding)),
                        bytes(v).decode(self.encoding),
                    )
                    for k, v in values
                ])

        pair_sets = [sorted(pairs, key=lambda x: x[0]) for pairs in pair_sets]

        configs = [
            make_dict_from_pairs(f"{_slash(scope_prefix)}{key_prefix}", pairs, "/")
            for scope_prefix, pairs in zip(scope_prefixes, pair_sets)
        ]
        return ChainMap(*configs)

    # for legacy
    get_prefix_dict = get_prefix

    async def replace(
        self,
        key: str,
        initial_val: str,
        new_val: str,
        *,
        scope: ConfigScopes = ConfigScopes.GLOBAL,
        scope_prefix_map: Mapping[ConfigScopes, str] = None,
    ) -> bool:
        scope_prefix = self._merge_scope_prefix_map(scope_prefix_map)[scope]
        mangled_key = self._mangle_key(f"{_slash(scope_prefix)}{key}")

        async with self.etcd.connect() as communicator:
            result = await communicator.txn(
                EtcdTransactionAction()
                .when([
                    Compare.value(
                        mangled_key.encode(self.encoding),
                        CompareOp.EQUAL,
                        initial_val.encode(self.encoding),
                    ),
                ])
                .and_then([
                    TxnOp.put(mangled_key.encode(self.encoding), new_val.encode(self.encoding))
                ])
                .or_else([])
            )

            return result.succeeded()

    async def delete(
        self,
        key: str,
        *,
        scope: ConfigScopes = ConfigScopes.GLOBAL,
        scope_prefix_map: Mapping[ConfigScopes, str] = None,
    ):
        scope_prefix = self._merge_scope_prefix_map(scope_prefix_map)[scope]
        mangled_key = self._mangle_key(f"{_slash(scope_prefix)}{key}")
        async with self.etcd.connect() as communicator:
            await communicator.delete(mangled_key.encode(self.encoding))

        async def delete_multi(
        self,
        keys: Iterable[str],
        *,
        scope: ConfigScopes = ConfigScopes.GLOBAL,
        scope_prefix_map: Mapping[ConfigScopes, str] = None,
    ):
        scope_prefix = self._merge_scope_prefix_map(scope_prefix_map)[scope]
        async with self.etcd.connect() as communicator:
            actions = []
            for k in keys:
                actions.append(
                    TxnOp.delete(
                        self._mangle_key(f"{_slash(scope_prefix)}{k}").encode(self.encoding)
                    )
                )
            await communicator.txn(EtcdTransactionAction().and_then(actions).or_else([]))

    async def delete_prefix(
        self,
        key_prefix: str,
        *,
        scope: ConfigScopes = ConfigScopes.GLOBAL,
        scope_prefix_map: Mapping[ConfigScopes, str] = None,
    ):
        scope_prefix = self._merge_scope_prefix_map(scope_prefix_map)[scope]
        mangled_key_prefix = self._mangle_key(f"{_slash(scope_prefix)}{key_prefix}")
        async with self.etcd.connect() as communicator:
            await communicator.delete_prefix(mangled_key_prefix.encode(self.encoding))