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


    async def close(self):
        pass

    def _mangle_key(self, k: str) -> str:
        if k.startswith("/"):
            k = k[1:]
        return f"/sorna/{self.ns}/{k}"