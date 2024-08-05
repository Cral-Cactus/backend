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

class SerializedExceptionFormatter(logging.Formatter):
    def formatException(self, ei) -> str:
        return format_exception(self, ei)


class GELFTLSHandler(graypy.GELFTLSHandler):
    ssl_ctx: ssl.SSLContext

    def __init__(self, host, port=12204, validate=False, ca_certs=None, **kwargs):
        super().__init__(host, port=port, validate=validate, **kwargs)
        self.ssl_ctx = ssl.create_default_context(capath=ca_certs)
        if not validate:
            self.ssl_ctx.check_hostname = False
            self.ssl_ctx.verify_mode = ssl.CERT_NONE

    def makeSocket(self, timeout=1):
        """Create a TLS wrapped socket"""
        plain_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

        if hasattr(plain_socket, "settimeout"):
            plain_socket.settimeout(timeout)

        wrapped_socket = self.ssl_ctx.wrap_socket(
            plain_socket,
            server_hostname=self.host,
        )
        wrapped_socket.connect((self.host, self.port))

        return wrapped_socket


def setup_graylog_handler(config: Mapping[str, Any]) -> Optional[logging.Handler]:
    drv_config = config["graylog"]
    graylog_params = {
        "host": drv_config["host"],
        "port": drv_config["port"],
        "validate": drv_config["ssl-verify"],
        "ca_certs": drv_config["ca-certs"],
        "keyfile": drv_config["keyfile"],
        "certfile": drv_config["certfile"],
    }
    if drv_config["localname"]:
        graylog_params["localname"] = drv_config["localname"]
    else:
        graylog_params["fqdn"] = drv_config["fqdn"]

    graylog_handler = GELFTLSHandler(**graylog_params)
    graylog_handler.setLevel(config["level"])
    return graylog_handler


class ConsoleFormatter(logging.Formatter):
    def formatException(self, ei) -> str:
        return format_exception(self, ei)

    def formatTime(self, record: logging.LogRecord, datefmt: str = None) -> str:
        ct = self.converter(record.created)
        if datefmt:
            datefmt = datefmt.replace("%f", f"{int(record.msecs):03d}")
            return time.strftime(datefmt, ct)
        else:
            t = time.strftime("%Y-%m-%d %H:%M:%S", ct)
            return f"{t}.{int(record.msecs):03d}"


class CustomJsonFormatter(JsonFormatter):
    def formatException(self, ei) -> str:
        return format_exception(self, ei)

    def add_fields(
        self,
        log_record: dict[str, Any],
        record: logging.LogRecord,
        message_dict: dict[str, Any],
    ) -> None:
        super().add_fields(log_record, record, message_dict)
        if not log_record.get("timestamp"):
            now = datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            log_record["timestamp"] = now
        if loglevel := log_record.get("level"):
            log_record["level"] = loglevel.upper()
        else:
            log_record["level"] = record.levelname.upper()


class ColorizedFormatter(coloredlogs.ColoredFormatter):
    def __init__(self, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        coloredlogs.logging.Formatter.formatException = format_exception


class pretty:
    def __init__(self, obj: Any) -> None:
        self.obj = obj

    def __repr__(self) -> str:
        return pprint.pformat(self.obj)


def setup_console_log_handler(config: Mapping[str, Any]) -> logging.Handler:
    log_formats = {
        "simple": "%(levelname)s %(message)s",
        "verbose": "%(asctime)s %(levelname)s %(name)s [%(process)d] %(message)s",
    }
    drv_config = config["console"]
    console_formatter: logging.Formatter
    colored = drv_config["colored"]
    if colored is None:
        colored = sys.stderr.isatty()
    if colored:
        console_formatter = ColorizedFormatter(
            log_formats[drv_config["format"]],
            datefmt="%Y-%m-%d %H:%M:%S.%f",
            field_styles={
                "levelname": {"color": 248, "bold": True},
                "name": {"color": 246, "bold": False},
                "process": {"color": "cyan"},
                "asctime": {"color": 240},
            },
            level_styles={
                "debug": {"color": "green"},
                "verbose": {"color": "green", "bright": True},
                "info": {"color": "cyan", "bright": True},
                "notice": {"color": "cyan", "bold": True},
                "warning": {"color": "yellow"},
                "error": {"color": "red", "bright": True},
                "success": {"color": 77},
                "critical": {"background": "red", "color": 255, "bold": True},
            },
        )
    else:
        console_formatter = ConsoleFormatter(
            log_formats[drv_config["format"]],
            datefmt="%Y-%m-%d %H:%M:%S.%f",
        )
    console_handler = logging.StreamHandler(
        stream=sys.stderr,
    )
    console_handler.setLevel(config["level"])
    console_handler.setFormatter(console_formatter)
    return console_handler


def setup_file_log_handler(config: Mapping[str, Any]) -> logging.Handler:
    drv_config = config["file"]
    fmt = "%(timestamp) %(level) %(name) %(processName) %(message)"
    file_handler = logging.handlers.RotatingFileHandler(
        filename=drv_config["path"] / drv_config["filename"],
        backupCount=drv_config["backup-count"],
        maxBytes=drv_config["rotation-size"],
        encoding="utf-8",
    )
    file_handler.setLevel(config["level"])
    file_handler.setFormatter(CustomJsonFormatter(fmt))
    return file_handler


def log_worker(
    logging_config: Mapping[str, Any],
    parent_pid: int,
    log_endpoint: str,
    ready_event: threading.Event,
) -> None:
    console_handler = None
    file_handler = None
    logstash_handler = None
    graylog_handler = None

    if "console" in logging_config["drivers"]:
        console_handler = setup_console_log_handler(logging_config)

    if "file" in logging_config["drivers"]:
        file_handler = setup_file_log_handler(logging_config)

    if "logstash" in logging_config["drivers"]:
        drv_config = logging_config["logstash"]
        logstash_handler = LogstashHandler(
            endpoint=drv_config["endpoint"],
            protocol=drv_config["protocol"],
            ssl_enabled=drv_config["ssl-enabled"],
            ssl_verify=drv_config["ssl-verify"],
            myhost="hostname",  # TODO: implement
        )
        logstash_handler.setLevel(logging_config["level"])
        logstash_handler.setFormatter(SerializedExceptionFormatter())
    if "graylog" in logging_config["drivers"]:
        graylog_handler = setup_graylog_handler(logging_config)
        assert graylog_handler is not None
        graylog_handler.setFormatter(SerializedExceptionFormatter())

    zctx = zmq.Context()
    agg_sock = zctx.socket(zmq.PULL)
    agg_sock.bind(log_endpoint)
    ep_url = yarl.URL(log_endpoint)
    if ep_url.scheme.lower() == "ipc":
        os.chmod(ep_url.path, 0o777)
    try:
        ready_event.set()
        while True:
            data = agg_sock.recv()
            if not data:
                return
            unpacked_data = msgpack.unpackb(data)
            if not unpacked_data:
                break
            rec = logging.makeLogRecord(unpacked_data)
            if rec is None:
                break
            if console_handler:
                console_handler.emit(rec)
            try:
                if file_handler:
                    file_handler.emit(rec)
                if logstash_handler:
                    logstash_handler.emit(rec)
                    print("logstash")
                if graylog_handler:
                    graylog_handler.emit(rec)
            except OSError:
                continue
    finally:
        if logstash_handler:
            logstash_handler.cleanup()
        if graylog_handler:
            graylog_handler.close()
        agg_sock.close()
        zctx.term()