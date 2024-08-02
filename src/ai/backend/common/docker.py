from __future__ import annotations

import enum
import functools
import ipaddress
import itertools
import json
import logging
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import (
    Any,
    Final,
    Iterable,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Union,
)

import aiohttp
import trafaret as t
import yarl
from packaging import version

from . import validators as tx
from .arch import arch_name_aliases
from .etcd import AsyncEtcd
from .etcd import quote as etcd_quote
from .etcd import unquote as etcd_unquote
from .exception import InvalidImageName, InvalidImageTag, UnknownImageRegistry
from .logging import BraceStyleAdapter
from .service_ports import parse_service_ports

__all__ = (
    "arch_name_aliases",
    "default_registry",
    "default_repository",
    "docker_api_arch_aliases",
    "common_image_label_schema",
    "inference_image_label_schema",
    "validate_image_labels",
    "login",
    "get_known_registries",
    "is_known_registry",
    "get_registry_info",
    "MIN_KERNELSPEC",
    "MAX_KERNELSPEC",
    "ImageRef",
)

docker_api_arch_aliases: Final[Mapping[str, str]] = {
    "aarch64": "arm64",
    "arm64": "arm64",
    "x86_64": "amd64",
    "x64": "amd64",
    "amd64": "amd64",
    "x86": "386",
    "x32": "386",
    "i686": "386",
    "386": "386",
}

log = BraceStyleAdapter(logging.getLogger(__spec__.name))

default_registry = "index.docker.io"
default_repository = "lablup"

MIN_KERNELSPEC = 1
MAX_KERNELSPEC = 1

common_image_label_schema = t.Dict({
    t.Key("ai.backend.kernelspec"): t.ToInt(lte=MAX_KERNELSPEC, gte=MIN_KERNELSPEC),
    t.Key("ai.backend.features"): tx.StringList(delimiter=" "),
    t.Key("ai.backend.base-distro"): t.String(),
    t.Key("ai.backend.runtime-type"): t.String(),
    t.Key("ai.backend.runtime-path"): tx.PurePath(),
    t.Key("ai.backend.role", default="COMPUTE"): t.Enum("COMPUTE", "INFERENCE", "SYSTEM"),
    t.Key("ai.backend.envs.corecount", optional=True): tx.StringList(allow_blank=True),
    t.Key("ai.backend.accelerators", optional=True): tx.StringList(allow_blank=True),
    t.Key("ai.backend.service-ports", optional=True): tx.StringList(allow_blank=True),
}).allow_extra("*")

inference_image_label_schema = t.Dict({
    t.Key("ai.backend.endpoint-ports"): tx.StringList(min_length=1),
    t.Key("ai.backend.model-path"): tx.PurePath(),
    t.Key("ai.backend.model-format"): t.String(),
}).ignore_extra("*")


class DockerConnectorSource(enum.Enum):
    ENV_VAR = enum.auto()
    USER_CONTEXT = enum.auto()
    KNOWN_LOCATION = enum.auto()

    @dataclass()
class DockerConnector:
    sock_path: Path | None
    docker_host: yarl.URL
    connector: aiohttp.BaseConnector
    source: DockerConnectorSource


@functools.lru_cache()
def _search_docker_socket_files_impl() -> (
    tuple[Path, yarl.URL, type[aiohttp.UnixConnector] | type[aiohttp.NamedPipeConnector]]
):
    connector_cls: type[aiohttp.UnixConnector] | type[aiohttp.NamedPipeConnector]
    match sys.platform:
        case "linux" | "darwin":
            search_paths = [
                Path("/run/docker.sock"),
                Path("/var/run/docker.sock"),
                Path.home() / ".docker/run/docker.sock",
            ]
            connector_cls = aiohttp.UnixConnector
        case "win32":
            search_paths = [
                Path(r"\\.\pipe\docker_engine"),
            ]
            connector_cls = aiohttp.NamedPipeConnector
        case _ as platform_name:
            raise RuntimeError(f"unsupported platform: {platform_name}")
    for p in search_paths:
        if p.exists() and (p.is_socket() or p.is_fifo()):
            return (
                p,
                yarl.URL("http://docker"),
                connector_cls,
            )
    else:
        searched_paths = ", ".join(map(os.fsdecode, search_paths))
        raise RuntimeError(f"could not find the docker socket; tried: {searched_paths}")


def search_docker_socket_files() -> tuple[Path | None, yarl.URL, aiohttp.BaseConnector]:
    connector_cls: type[aiohttp.UnixConnector] | type[aiohttp.NamedPipeConnector]
    sock_path, docker_host, connector_cls = _search_docker_socket_files_impl()
    return (
        sock_path,
        docker_host,
        connector_cls(os.fsdecode(sock_path), force_close=True),
    )


def get_docker_connector() -> DockerConnector:
    if raw_docker_host := os.environ.get("DOCKER_HOST", None):
        sock_path, docker_host, connector = parse_docker_host_url(yarl.URL(raw_docker_host))
        return DockerConnector(
            sock_path,
            docker_host,
            connector,
            DockerConnectorSource.ENV_VAR,
        )
    if raw_docker_host := get_docker_context_host():
        sock_path, docker_host, connector = parse_docker_host_url(yarl.URL(raw_docker_host))
        return DockerConnector(
            sock_path,
            docker_host,
            connector,
            DockerConnectorSource.USER_CONTEXT,
        )
    sock_path, docker_host, connector = search_docker_socket_files()
    return DockerConnector(
        sock_path,
        docker_host,
        connector,
        DockerConnectorSource.KNOWN_LOCATION,
    )


async def login(
    sess: aiohttp.ClientSession, registry_url: yarl.URL, credentials: dict, scope: str
) -> dict:
    basic_auth: Optional[aiohttp.BasicAuth]

    if credentials.get("username") and credentials.get("password"):
        basic_auth = aiohttp.BasicAuth(
            credentials["username"],
            credentials["password"],
        )
    else:
        basic_auth = None
    realm = registry_url / "token"  # fallback
    service = "registry"  # fallback
    async with sess.get(registry_url / "v2/", auth=basic_auth) as resp:
        ping_status = resp.status
        www_auth_header = resp.headers.get("WWW-Authenticate")
        if www_auth_header:
            match = re.search(r'realm="([^"]+)"', www_auth_header)
            if match:
                realm = yarl.URL(match.group(1))
            match = re.search(r'service="([^"]+)"', www_auth_header)
            if match:
                service = match.group(1)
    if ping_status == 200:
        log.debug("docker-registry: {0} -> basic-auth", registry_url)
        return {"auth": basic_auth, "headers": {}}
    elif ping_status == 404:
        raise RuntimeError(f"Unsupported docker registry: {registry_url}! (API v2 not implemented)")
    elif ping_status == 401:
        params = {
            "scope": scope,
            "offline_token": "true",
            "client_id": "docker",
            "service": service,
        }
        async with sess.get(realm, params=params, auth=basic_auth) as resp:
            log.debug("docker-registry: {0} -> {1}", registry_url, realm)
            if resp.status == 200:
                data = json.loads(await resp.read())
                token = data.get("token", None)
                return {
                    "auth": None,
                    "headers": {
                        "Authorization": f"Bearer {token}",
                    },
                }
    raise RuntimeError(f"authentication for docker registry {registry_url} failed")


async def get_known_registries(etcd: AsyncEtcd) -> Mapping[str, yarl.URL]:
    data = await etcd.get_prefix("config/docker/registry/")
    results: MutableMapping[str, yarl.URL] = {}
    for key, value in data.items():
        name = etcd_unquote(key)
        if isinstance(value, str):
            results[name] = yarl.URL(value)
        elif isinstance(value, Mapping):
            assert isinstance(value[""], str)
            results[name] = yarl.URL(value[""])
    return results


def is_known_registry(
    val: str,
    known_registries: Union[Mapping[str, Any], Sequence[str]] | None = None,
):
    if val == default_registry:
        return True
    if known_registries is not None and val in known_registries:
        return True
    try:
        url = yarl.URL("//" + val)
        if url.host and ipaddress.ip_address(url.host):
            return True
    except ValueError:
        pass
    return False


async def get_registry_info(etcd: AsyncEtcd, name: str) -> tuple[yarl.URL, dict]:
    reg_path = f"config/docker/registry/{etcd_quote(name)}"
    item = await etcd.get_prefix(reg_path)
    if not item:
        raise UnknownImageRegistry(name)
    registry_addr = item[""]
    if not registry_addr:
        raise UnknownImageRegistry(name)
    assert isinstance(registry_addr, str)
    creds = {}
    username = item.get("username")
    if username is not None:
        creds["username"] = username
    password = item.get("password")
    if password is not None:
        creds["password"] = password
    return yarl.URL(registry_addr), creds


def validate_image_labels(labels: dict[str, str]) -> dict[str, str]:
    common_labels = common_image_label_schema.check(labels)
    service_ports = {
        item["name"]: item
        for item in parse_service_ports(
            common_labels.get("ai.backend.service-ports", ""),
            common_labels.get("ai.backend.endpoint-ports", ""),
        )
    }
    match common_labels["ai.backend.role"]:
        case "INFERENCE":
            inference_labels = inference_image_label_schema.check(labels)
            for name in inference_labels["ai.backend.endpoint-ports"]:
                if name not in service_ports:
                    raise ValueError(
                        f"ai.backend.endpoint-ports contains an undefined service port: {name}"
                    )
                if service_ports[name]["protocol"] != "preopen":
                    raise ValueError(f"The endpoint-port {name} must be a preopen service-port.")
            common_labels.update(inference_labels)
        case _:
            pass
    return common_labels


class PlatformTagSet(Mapping):
    __slots__ = ("_data",)
    _data: dict[str, str]
    _rx_ver = re.compile(r"^(?P<tag>[a-zA-Z_]+)(?P<version>\d+(?:\.\d+)*[a-z0-9]*)?$")

    def __init__(self, tags: Iterable[str], value: str = None) -> None:
        self._data = dict()
        rx = type(self)._rx_ver
        for tag in tags:
            match = rx.search(tag)
            if match is None:
                raise InvalidImageTag(tag, value)
            key = match.group("tag")
            value = match.group("version")
            if key in self._data:
                raise InvalidImageTag(tag, value)
            if value is None:
                value = ""
            self._data[key] = value