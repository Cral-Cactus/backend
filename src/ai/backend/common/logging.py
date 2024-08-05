import json
import logging
import logging.config
import logging.handlers
import os
import pprint
import socket
import ssl
import sys
import threading
import time
import traceback
from abc import ABCMeta, abstractmethod
from collections import OrderedDict
from contextvars import ContextVar
from datetime import datetime
from pathlib import Path
from typing import Any, Mapping, MutableMapping, Optional

import coloredlogs
import graypy
import trafaret as t
import yarl
import zmq
from pythonjsonlogger.jsonlogger import JsonFormatter

from ai.backend.common import msgpack

from . import config
from . import validators as tx
from .exception import ConfigurationError
from .logging_utils import BraceStyleAdapter

__all__ = (
    "AbstractLogger",
    "Logger",
    "NoopLogger",
    "BraceStyleAdapter",
    "LogstashHandler",
    "is_active",
    "pretty",
)

is_active: ContextVar[bool] = ContextVar("is_active", default=False)

loglevel_iv = t.Enum("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL", "NOTSET")
logformat_iv = t.Enum("simple", "verbose")
default_pkg_ns = {
    "": "WARNING",
    "ai.backend": "INFO",
    "tests": "DEBUG",
}
logging_config_iv = t.Dict({
    t.Key("level", default="INFO"): loglevel_iv,
    t.Key("pkg-ns", default=default_pkg_ns): t.Mapping(t.String(allow_blank=True), loglevel_iv),
    t.Key("drivers", default=["console"]): t.List(t.Enum("console", "logstash", "file", "graylog")),
    t.Key(
        "console",
        default={
            "colored": None,
            "format": "verbose",
        },
    ): t.Dict({
        t.Key("colored", default=None): t.Null | t.Bool,
        t.Key("format", default="verbose"): logformat_iv,
    }).allow_extra("*"),
    t.Key("file", default=None): t.Null
    | t.Dict({
        t.Key("path"): tx.Path(type="dir", auto_create=True),
        t.Key("filename"): t.String,
        t.Key("backup-count", default=5): t.Int[1:100],
        t.Key("rotation-size", default="10M"): tx.BinarySize,
        t.Key("format", default="verbose"): logformat_iv,
    }).allow_extra("*"),
    t.Key("logstash", default=None): t.Null
    | t.Dict({
        t.Key("endpoint"): tx.HostPortPair,
        t.Key("protocol", default="tcp"): t.Enum("zmq.push", "zmq.pub", "tcp", "udp"),
        t.Key("ssl-enabled", default=True): t.Bool,
        t.Key("ssl-verify", default=True): t.Bool,
    }).allow_extra("*"),
    t.Key("graylog", default=None): t.Null
    | t.Dict({
        t.Key("host"): t.String,
        t.Key("port"): t.ToInt[1024:65535],
        t.Key("level", default="INFO"): loglevel_iv,
        t.Key("ssl-verify", default=False): t.Bool,
        t.Key("ca-certs", default=None): t.Null | t.String(allow_blank=True),
        t.Key("keyfile", default=None): t.Null | t.String(allow_blank=True),
        t.Key("certfile", default=None): t.Null | t.String(allow_blank=True),
        t.Key("fqdn", default=True): t.Bool,
        t.Key("localname", default=None): t.Null | t.String(),
    }).allow_extra("*"),
}).allow_extra("*")


class PickledException(Exception):
    pass


class LogstashHandler(logging.Handler):
    def __init__(
        self,
        endpoint,
        protocol: str,
        *,
        ssl_enabled: bool = True,
        ssl_verify: bool = True,
        myhost: str = None,
    ):
        super().__init__()
        self._endpoint = endpoint
        self._protocol = protocol
        self._ssl_enabled = ssl_enabled
        self._ssl_verify = ssl_verify
        self._myhost = myhost
        self._sock = None
        self._sslctx = None
        self._zmqctx = None

    def _setup_transport(self):
        if self._sock is not None:
            return
        if self._protocol == "zmq.push":
            self._zmqctx = zmq.Context()
            sock = self._zmqctx.socket(zmq.PUSH)
            sock.setsockopt(zmq.LINGER, 50)
            sock.setsockopt(zmq.SNDHWM, 20)
            sock.connect(f"tcp://{self._endpoint[0]}:{self._endpoint[1]}")
            self._sock = sock
        elif self._protocol == "zmq.pub":
            self._zmqctx = zmq.Context()
            sock = self._zmqctx.socket(zmq.PUB)
            sock.setsockopt(zmq.LINGER, 50)
            sock.setsockopt(zmq.SNDHWM, 20)
            sock.connect(f"tcp://{self._endpoint[0]}:{self._endpoint[1]}")
            self._sock = sock
        elif self._protocol == "tcp":
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if self._ssl_enabled:
                self._sslctx = ssl.create_default_context()
                if not self._ssl_verify:
                    self._sslctx.check_hostname = False
                    self._sslctx.verify_mode = ssl.CERT_NONE
                sock = self._sslctx.wrap_socket(sock, server_hostname=self._endpoint[0])
            sock.connect((str(self._endpoint.host), self._endpoint.port))
            self._sock = sock
        elif self._protocol == "udp":
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.connect((str(self._endpoint.host), self._endpoint.port))
            self._sock = sock
        else:
            raise ConfigurationError({
                "logging.LogstashHandler": f"unsupported protocol: {self._protocol}"
            })

    def cleanup(self):
        if self._sock:
            self._sock.close()
        self._sslctx = None
        if self._zmqctx:
            self._zmqctx.term()

    def emit(self, record):
        self._setup_transport()
        tags = set()
        extra_data = dict()

        log = OrderedDict([
            ("@timestamp", datetime.now().isoformat()),
            ("@version", 1),
            ("host", self._myhost),
            ("logger", record.name),
            ("path", record.pathname),
            ("func", record.funcName),
            ("lineno", record.lineno),
            ("message", record.getMessage()),
            ("level", record.levelname),
            ("tags", list(tags)),
        ])
        log.update(extra_data)
        if self._protocol.startswith("zmq"):
            self._sock.send_json(log)
        else:
            self._sock.sendall(json.dumps(log).encode("utf-8"))


def format_exception(self, ei):
    s = "".join(ei)
    if s[-1:] == "\n":
        s = s[:-1]
    return s