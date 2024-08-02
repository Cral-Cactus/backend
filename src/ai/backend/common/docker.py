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
def get_docker_context_host() -> str | None:
    try:
        docker_config_path = Path.home() / ".docker" / "config.json"
        docker_config = json.loads(docker_config_path.read_bytes())
    except IOError:
        return None
    current_context_name = docker_config.get("currentContext", "default")
    for meta_path in (Path.home() / ".docker" / "contexts" / "meta").glob("*/meta.json"):
        context_data = json.loads(meta_path.read_bytes())
        if context_data["Name"] == current_context_name:
            return context_data["Endpoints"]["docker"]["Host"]
    return None


def parse_docker_host_url(
    docker_host: yarl.URL,
) -> tuple[Path | None, yarl.URL, aiohttp.BaseConnector]:
    connector_cls: type[aiohttp.UnixConnector] | type[aiohttp.NamedPipeConnector]
    match docker_host.scheme:
        case "http" | "https":
            return None, docker_host, aiohttp.TCPConnector()
        case "unix":
            path = Path(docker_host.path)
            if not path.exists() or not path.is_socket():
                raise RuntimeError(f"DOCKER_HOST {path} is not a valid socket file.")
            decoded_path = os.fsdecode(path)
            connector_cls = aiohttp.UnixConnector
        case "npipe":
            path = Path(docker_host.path.replace("/", "\\"))
            if not path.exists() or not path.is_fifo():
                raise RuntimeError(f"DOCKER_HOST {path} is not a valid named pipe.")
            decoded_path = os.fsdecode(path)
            connector_cls = aiohttp.NamedPipeConnector
        case _ as unknown_scheme:
            raise RuntimeError("unsupported connection scheme", unknown_scheme)
    return (
        path,
        yarl.URL("http://docker"),
        connector_cls(decoded_path, force_close=True),
    )