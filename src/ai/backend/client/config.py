import enum
import os
import random
import re
from pathlib import Path
from typing import (
    Any,
    Callable,
    Iterable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    TypeVar,
    Union,
    cast,
)

import appdirs
from dotenv import find_dotenv, load_dotenv
from yarl import URL

__all__ = [
    "parse_api_version",
    "get_config",
    "set_config",
    "APIConfig",
    "API_VERSION",
    "DEFAULT_CHUNK_SIZE",
    "MAX_INFLIGHT_CHUNKS",
]


class Undefined(enum.Enum):
    token = object()


_config = None
_undefined = Undefined.token

API_VERSION = (8, "20240315")
MIN_API_VERSION = (7, "20230615")

DEFAULT_CHUNK_SIZE = 16 * (2**20)  # 16 MiB
MAX_INFLIGHT_CHUNKS = 4


def parse_api_version(value: str) -> Tuple[int, str]:
    match = re.search(r"^v(?P<major>\d+)\.(?P<date>\d{8})$", value)
    if match is not None:
        return int(match.group(1)), match.group(2)
    raise ValueError("Could not parse the given API version string", value)


T = TypeVar("T")


def default_clean(v: T | Any) -> T:
    return cast(T, v)


def get_env(
    key: str,
    default: Union[str, Mapping, Undefined] = _undefined,
    *,
    clean: Callable[[Any], T] = default_clean,
) -> T:

    load_dotenv(dotenv_path=find_dotenv(usecwd=True), override=True)
    key = key.upper()
    raw = os.environ.get("BACKEND_" + key)
    if raw is None:
        raw = os.environ.get("SORNA_" + key)
    if raw is None:
        if default is _undefined:
            raise KeyError(key)
        result = default
    else:
        result = raw
    return clean(result)


def bool_env(v: str) -> bool:
    v = v.lower()
    if v in ("y", "yes", "t", "true", "1"):
        return True
    if v in ("n", "no", "f", "false", "0"):
        return False
    raise ValueError("Unrecognized value of boolean environment variable", v)


def _clean_urls(v: Union[URL, str]) -> List[URL]:
    if isinstance(v, URL):
        return [v]
    urls = []
    if isinstance(v, str):
        for entry in v.split(","):
            url = URL(entry)
            if not url.is_absolute():
                raise ValueError("URL {} is not absolute.".format(url))
            urls.append(url)
    return urls


def _clean_tokens(v: str) -> Tuple[str, ...]:
    if not v:
        return tuple()
    return tuple(v.split(","))


def _clean_address_map(v: Union[str, Mapping]) -> Mapping:
    if isinstance(v, dict):
        return v
    if not isinstance(v, str):
        raise ValueError(
            f'Storage proxy address map has invalid type "{type(v)}", expected str or dict.',
        )
    override_map = {}
    for assignment in v.split(","):
        try:
            k, _, v = assignment.partition("=")
            if k == "" or v == "":
                raise ValueError
        except ValueError:
            raise ValueError(f"{v} is not a valid mapping expression")
        else:
            override_map[k] = v
    return override_map


class APIConfig:
    DEFAULTS: Mapping[str, Union[str, Mapping]] = {
        "endpoint_type": "api",
        "version": f"v{API_VERSION[0]}.{API_VERSION[1]}",
        "hash_type": "sha256",
        "domain": "default",
        "group": "default",
        "storage_proxy_address_map": {},
        "connection_timeout": "10.0",
        "read_timeout": "0",
    }

    _endpoints: List[URL]
    _group: str
    _hash_type: str
    _skip_sslcert_validation: bool
    _version: str

    def __init__(
        self,
        *,
        endpoint: Union[URL, str] = None,
        endpoint_type: str = None,
        domain: str = None,
        group: str = None,
        storage_proxy_address_map: Mapping[str, str] = None,
        version: str = None,
        user_agent: str = None,
        access_key: str = None,
        secret_key: str = None,
        hash_type: str = None,
        vfolder_mounts: Iterable[str] = None,
        skip_sslcert_validation: bool = None,
        connection_timeout: float = None,
        read_timeout: float = None,
        announcement_handler: Callable[[str], None] = None,
    ) -> None:
        from . import get_user_agent

        self._endpoints = (
            _clean_urls(endpoint)
            if endpoint
            else get_env("ENDPOINT", self.DEFAULTS["endpoint"], clean=_clean_urls)
        )
        random.shuffle(self._endpoints)
        self._endpoint_type = (
            endpoint_type
            if endpoint_type is not None
            else get_env("ENDPOINT_TYPE", self.DEFAULTS["endpoint_type"], clean=str)
        )
        self._domain = (
            domain if domain is not None else get_env("DOMAIN", self.DEFAULTS["domain"], clean=str)
        )