from __future__ import annotations

import asyncio
import functools
import logging
from collections import ChainMap, namedtuple
from typing import (
    AsyncGenerator,
    AsyncIterator,
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

import grpc
import trafaret as t
from etcetra import EtcdCommunicator, WatchEvent
from etcetra.client import EtcdClient, EtcdTransactionAction
from etcetra.types import CompareKey, EtcdCredential
from etcetra.types import HostPortPair as EtcetraHostPortPair

from .etcd import ConfigScopes
from .logging_utils import BraceStyleAdapter
from .types import HostPortPair, QueueSentinel

__all__ = (
    "quote",
    "unquote",
    "AsyncEtcd",
)

Event = namedtuple("Event", "key event value")

log = BraceStyleAdapter(logging.getLogger(__spec__.name))


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

    _creds: Optional[EtcdCredential]

    def __init__(
        self,
        addr: HostPortPair | EtcetraHostPortPair,
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
            self._creds = EtcdCredential(credentials["user"], credentials["password"])
        else:
            self._creds = None

        self.ns = namespace
        log.info('using etcd cluster from {} with namespace "{}"', addr, namespace)
        self.encoding = encoding
        self.watch_reconnect_intvl = watch_reconnect_intvl
        self.etcd = EtcdClient(
            EtcetraHostPortPair(str(addr.host), addr.port),
            credentials=self._creds,
            encoding=self.encoding,
        )

    async def close(self):
        pass

    def _mangle_key(self, k: str) -> str:
        if k.startswith("/"):
            k = k[1:]
        return f"/sorna/{self.ns}/{k}"

    def _demangle_key(self, k: Union[bytes, str]) -> str:
        if isinstance(k, bytes):
            k = k.decode(self.encoding)
        prefix = f"/sorna/{self.ns}/"
        if k.startswith(prefix):
            k = k[len(prefix) :]
        return k