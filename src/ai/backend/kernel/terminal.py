import argparse
import asyncio
import fcntl
import logging
import os
import pty
import shlex
import signal
import struct
import sys
import termios
import traceback

import zmq
import zmq.asyncio

from .logging import BraceStyleAdapter
from .utils import safe_close_task

log = BraceStyleAdapter(logging.getLogger())


class Terminal:
    def __init__(self, shell_cmd, ev_term, sock_out, *, auto_restart=True, loop=None):
        self._sorna_media = []
        self.zctx = sock_out.context

        self.ev_term = ev_term
        self.pid = None
        self.fd = None

        self.shell_cmd = shell_cmd
        self.auto_restart = auto_restart

        self.sock_out = sock_out

        self.sock_term_in = None
        self.sock_term_out = None
        self.term_in_task = None
        self.term_out_task = None
        self.start_lock = asyncio.Lock()
        self.accept_term_input = False

        self.cmdparser = argparse.ArgumentParser()
        self.subparsers = self.cmdparser.add_subparsers()

        parser_ping = self.subparsers.add_parser("ping")
        parser_ping.set_defaults(func=self.do_ping)

        parser_resize = self.subparsers.add_parser("resize")
        parser_resize.add_argument("rows", type=int)
        parser_resize.add_argument("cols", type=int)
        parser_resize.set_defaults(func=self.do_resize_term)

    async def do_ping(self, args) -> int:
        await self.sock_out.send_multipart([b"stdout", b"pong!"])
        return 0

    async def do_resize_term(self, args) -> int:
        if self.fd is None:
            return 0
        origsz_in = struct.pack("HHHH", 0, 0, 0, 0)
        origsz_out = fcntl.ioctl(self.fd, termios.TIOCGWINSZ, origsz_in, False)
        orig_lines, orig_cols, _, _ = struct.unpack("HHHH", origsz_out)
        newsz_in = struct.pack("HHHH", args.rows, args.cols, orig_lines, orig_cols)
        newsz_out = fcntl.ioctl(self.fd, termios.TIOCSWINSZ, newsz_in, False)
        new_lines, new_cols, _, _ = struct.unpack("HHHH", newsz_out)
        await self.sock_out.send_multipart([
            b"stdout",
            f"OK; terminal resized to {new_lines} lines and {new_cols} columns".encode(),
        ])
        return 0

    async def handle_command(self, code_txt) -> int:
        try:
            if code_txt.startswith("%"):
                args = self.cmdparser.parse_args(shlex.split(code_txt[1:], comments=True))
                if asyncio.iscoroutine(args.func) or asyncio.iscoroutinefunction(args.func):
                    return await args.func(args)
                else:
                    return args.func(args)
            else:
                await self.sock_out.send_multipart([b"stderr", b"Invalid command."])
                return 127
        except Exception:
            exc_type, exc_val, tb = sys.exc_info()
            traces = traceback.format_exception(exc_type, exc_val, tb)
            await self.sock_out.send_multipart([b"stderr", "".join(traces).encode()])
            return 1
        finally:
            await self.sock_out.send_multipart([b"finished", b"{}"])

    async def start(self):
        assert not self.accept_term_input
        await safe_close_task(self.term_in_task)
        await safe_close_task(self.term_out_task)
        pid, fd = pty.fork()
        if pid == 0:
            args = shlex.split(self.shell_cmd)
            os.execv(args[0], args)
        else:
            self.pid = pid
            self.fd = fd

            if self.sock_term_in is None:
                self.sock_term_in = self.zctx.socket(zmq.SUB)
                self.sock_term_in.bind("tcp://*:2002")
                self.sock_term_in.subscribe(b"")
            if self.sock_term_out is None:
                self.sock_term_out = self.zctx.socket(zmq.PUB)
                self.sock_term_out.bind("tcp://*:2003")

            loop = asyncio.get_running_loop()
            term_reader = asyncio.StreamReader()
            term_read_protocol = asyncio.StreamReaderProtocol(term_reader)
            await loop.connect_read_pipe(lambda: term_read_protocol, os.fdopen(self.fd, "rb"))

            _reader_factory = lambda: asyncio.StreamReaderProtocol(asyncio.StreamReader())
            term_writer_transport, term_writer_protocol = await loop.connect_write_pipe(
                _reader_factory, os.fdopen(self.fd, "wb")
            )
            term_writer = asyncio.StreamWriter(term_writer_transport, term_writer_protocol, None)

            self.term_in_task = asyncio.create_task(self.term_in(term_writer))
            self.term_out_task = asyncio.create_task(self.term_out(term_reader))
            self.accept_term_input = True
            await asyncio.sleep(0)