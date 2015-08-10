import asyncio
import logging
import os
import signal
from struct import Struct
import time

from .base import (WorkerProcess, ChildProcess,
                   IDLE_CHECK, IDLE_TIME)

MSG_HEAD = 0x0
MSG_PING = 0x1
MSG_PONG = 0x2
MSG_CLOSE = 0x3

PACK_MSG = Struct('!BB').pack
UNPACK_MSG = Struct('!BB').unpack


logger = logging.getLogger(__name__)


class ConnectionClosedError(Exception):
    pass


@asyncio.coroutine
def connect_write_pipe(file):
    loop = asyncio.get_event_loop()
    transport, _ = yield from loop.connect_write_pipe(asyncio.Protocol, file)
    return PipeWriter(transport)


@asyncio.coroutine
def connect_read_pipe(file):
    loop = asyncio.get_event_loop()
    pipe_reader = PipeReader(loop=loop)
    transport, _ = yield from loop.connect_read_pipe(
        lambda: PipeReadProtocol(pipe_reader), file)
    pipe_reader.transport = transport
    return pipe_reader


class PipeWriter:

    def __init__(self, transport):
        self.transport = transport

    def _send(self, msg):
        self.transport.write(PACK_MSG(MSG_HEAD, msg))

    def ping(self):
        self._send(MSG_PING)

    def pong(self):
        self._send(MSG_PONG)

    def stop(self):
        self._send(MSG_CLOSE)

    def close(self):
        if self.transport is not None:
            self.transport.close()


class PipeReadProtocol(asyncio.Protocol):

    def __init__(self, reader):
        self.reader = reader

    def data_received(self, data):
        self.reader.feed(data)

    def connection_lost(self, exc):
        self.reader.close()


class PipeReader:

    closed = False
    transport = None

    def __init__(self, loop):
        self.loop = loop
        self._waiters = asyncio.Queue()

    def close(self):
        self.closed = True
        while not self._waiters.empty():
            waiter = self._waiters.get_nowait()
            if not waiter.done():
                waiter.set_exception(ConnectionClosedError())
        if self.transport is not None:
            self.transport.close()

    def feed(self, data):
        asyncio.async(self._feed_waiter(data))

    @asyncio.coroutine
    def _feed_waiter(self, data):
        waiter = yield from self._waiters.get()
        waiter.set_result(data)

    @asyncio.coroutine
    def read(self):
        if self.closed:
            raise ConnectionClosedError()
        waiter = asyncio.Future(loop=self.loop)
        yield from self._waiters.put(waiter)
        data = yield from waiter

        hdr, msg = UNPACK_MSG(data)
        if hdr == MSG_HEAD:
            return msg


class ForkChild(ChildProcess):

    _heartbeat_task = None

    def __init__(self, parent_read, parent_write, loader, **options):
        ChildProcess.__init__(self, loader, **options)
        self.parent_read = parent_read
        self.parent_write = parent_write

    @asyncio.coroutine
    def on_start(self):
        self._heartbeat_task = asyncio.Task(self.heartbeat())

    def stop(self):
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
        ChildProcess.stop(self)

    @asyncio.coroutine
    def heartbeat(self):
        # setup pipes
        reader = yield from connect_read_pipe(
            os.fdopen(self.parent_read, 'rb'))
        writer = yield from connect_write_pipe(
            os.fdopen(self.parent_write, 'wb'))

        while True:
            try:
                msg = yield from reader.read()
            except ConnectionClosedError:
                logger.info('Parent is dead, {} stopping...'
                            ''.format(os.getpid()))
                break

            if msg == MSG_PING:
                writer.pong()
            elif msg.tp == MSG_CLOSE:
                break

        reader.close()
        writer.close()
        self.stop()


class ForkWorker(WorkerProcess):

    pid = ping = None
    reader = writer = None
    chat_task = heartbeat_task = None

    def start_child(self):

        parent_read, child_write = os.pipe()
        child_read, parent_write = os.pipe()

        pid = os.fork()
        if pid:
            # parent
            os.close(parent_read)
            os.close(parent_write)
            asyncio.async(self.connect(pid, child_write, child_read))
        else:
            # child
            os.close(child_write)
            os.close(child_read)

            # cleanup after fork
            asyncio.set_event_loop(None)
            # setup process
            process = ForkChild(parent_read, parent_write, self.loader)
            process.start()

    def kill_child(self):
        self.chat_task.cancel()
        self.heartbeat_task.cancel()
        self.reader.close()
        self.writer.close()
        try:
            os.kill(self.pid, signal.SIGTERM)
            os.waitpid(self.pid, 0)
        except ProcessLookupError:
            pass

    @asyncio.coroutine
    def heartbeat(self, writer):
        idle_time = self.options.get('idle_time', IDLE_TIME)
        idle_check = self.options.get('idle_check', IDLE_CHECK)
        while True:
            yield from asyncio.sleep(idle_check)

            if (time.monotonic() - self.ping) < idle_time:
                writer.ping()
            else:
                self.restart()
                return

    @asyncio.coroutine
    def chat(self, reader):
        while True:
            try:
                msg = yield from reader.read()
            except ConnectionClosedError:
                self.restart()
                return

            if msg == MSG_PONG:
                self.ping = time.monotonic()

    @asyncio.coroutine
    def connect(self, pid, up_write, down_read):
        # setup pipes
        reader = yield from connect_read_pipe(
            os.fdopen(down_read, 'rb'))
        writer = yield from connect_write_pipe(
            os.fdopen(up_write, 'wb'))

        # store info
        self.pid = pid
        self.ping = time.monotonic()
        self.reader = reader
        self.writer = writer
        self.chat_task = asyncio.Task(self.chat(reader))
        self.heartbeat_task = asyncio.Task(self.heartbeat(writer))
