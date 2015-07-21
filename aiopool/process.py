import asyncio
import logging
from functools import partial
import os
import signal
from struct import Struct
import time


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
    return transport, PipeWriter(transport)


@asyncio.coroutine
def connect_read_pipe(file):
    loop = asyncio.get_event_loop()
    pipe_reader = PipeReader(loop=loop)
    transport, _ = yield from loop.connect_read_pipe(
        lambda: PipeReadProtocol(pipe_reader), file)
    return transport, pipe_reader


class PipeWriter:

    def __init__(self, transport):
        self._transport = transport

    def _send(self, msg):
        self._transport.write(PACK_MSG(MSG_HEAD, msg))

    def ping(self):
        self._send(MSG_PING)

    def pong(self):
        self._send(MSG_PONG)

    def stop(self):
        self._send(MSG_CLOSE)


class PipeReadProtocol(asyncio.Protocol):

    def __init__(self, reader):
        self.reader = reader

    def data_received(self, data):
        self.reader.feed(data)

    def connection_lost(self, exc):
        self.reader.close()


class PipeReader:

    closed = False

    def __init__(self, loop):
        self.loop = loop
        self._waiters = asyncio.Queue()

    def close(self):
        self.closed = True
        while not self._waiters.empty():
            waiter = self._waiters.get_nowait()
            if not waiter.done():
                waiter.set_exception(ConnectionClosedError())

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


class ChildProcess:

    loop = None
    _on_start_return = None

    def __init__(self, up_read, down_write,
                 on_start=None, on_stop=None):
        self.on_start = on_start
        self.on_stop = on_stop
        self.up_read = up_read
        self.down_write = down_write

    def start(self):
        self.loop = loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        if self.on_start is not None:
            def cb_on_start(fut):
                self._on_start_return = fut.result()
            start_task = asyncio.async(self.on_start())
            start_task.add_done_callback(cb_on_start)

        asyncio.async(self.heartbeat())
        loop.add_signal_handler(signal.SIGINT, self.stop)
        loop.add_signal_handler(signal.SIGTERM, self.stop)

        asyncio.get_event_loop().run_forever()

        if self.on_stop is not None:
            args = []
            if self._on_start_return:
                args.append(self._on_start_return)
            loop.run_until_complete(self.on_stop(*args))

        loop.remove_signal_handler(signal.SIGINT)
        loop.remove_signal_handler(signal.SIGTERM)
        os._exit(0)

    def stop(self):
        if self.loop.is_running():
            self.loop.stop()

    @asyncio.coroutine
    def heartbeat(self):
        # setup pipes
        read_transport, reader = yield from connect_read_pipe(
            os.fdopen(self.up_read, 'rb'))
        write_transport, writer = yield from connect_write_pipe(
            os.fdopen(self.down_write, 'wb'))

        while True:
            try:
                msg = yield from reader.read()
            except ConnectionClosedError:
                logger.info('Parent is dead, {} stopping...'
                            ''.format(os.getpid()))
                self.stop()
                break

            if msg == MSG_PING:
                writer.pong()
            elif msg.tp == MSG_CLOSE:
                break

        read_transport.close()
        write_transport.close()


class Worker:

    _started = False

    pid, ping = None, None
    r_transport, w_transport = None, None
    chat_task, heartbeat_task = None, None

    restarting = False

    def __init__(self, loop, on_start=None, on_stop=None):
        self.loop = loop
        self.on_start = on_start
        self.on_stop = on_stop
        self.start()

    def start(self):
        assert not self._started
        self._started = True

        up_read, up_write = os.pipe()
        down_read, down_write = os.pipe()

        pid = os.fork()
        if pid:
            # parent
            os.close(up_read)
            os.close(down_write)
            asyncio.async(self.connect(pid, up_write, down_read))
        else:
            # child
            os.close(up_write)
            os.close(down_read)

            # cleanup after fork
            asyncio.set_event_loop(None)
            # setup process
            process = ChildProcess(up_read, down_write,
                                   self.on_start, self.on_stop)
            process.start()

    def kill(self):
        self._started = False
        self.chat_task.cancel()
        self.heartbeat_task.cancel()
        self.r_transport.close()
        self.w_transport.close()
        try:
            os.kill(self.pid, signal.SIGTERM)
            os.waitpid(self.pid, 0)
        except ProcessLookupError:
            pass

    @asyncio.coroutine
    def heartbeat(self, writer):
        while True:
            yield from asyncio.sleep(15)

            if (time.monotonic() - self.ping) < 30:
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
        read_transport, reader = yield from connect_read_pipe(
            os.fdopen(down_read, 'rb'))
        write_transport, writer = yield from connect_write_pipe(
            os.fdopen(up_write, 'wb'))

        # store info
        self.pid = pid
        self.ping = time.monotonic()
        self.r_transport = read_transport
        self.w_transport = write_transport
        self.chat_task = asyncio.Task(self.chat(reader))
        self.heartbeat_task = asyncio.Task(self.heartbeat(writer))

    def restart(self):
        if not self.restarting:
            logger.info('Restart worker process: {}'.format(self.pid))
            self.restarting = True
            self.kill()
            self.start()
            self.restarting = False


class subprocess:

    def __init__(self, on_start=None, on_stop=None):
        self._on_start = on_start
        self._on_stop = on_stop

    def on_start(self, fn):
        self._on_start = fn

    def on_stop(self, fn):
        self._on_stop = fn

    def __call__(self, *args, **kwargs):
        loop = asyncio.get_event_loop()

        on_start_coro = self._on_start
        if (on_start_coro is not None
                and not asyncio.iscoroutinefunction(on_start_coro)):
            on_start_coro = asyncio.coroutine(on_start_coro)
        on_start_coro = partial(on_start_coro, *args, **kwargs)

        on_stop_coro = self._on_stop
        if (on_stop_coro is not None
                and not asyncio.iscoroutinefunction(on_stop_coro)):
            on_stop_coro = asyncio.coroutine(on_stop_coro)

        return Worker(loop, on_start=on_start_coro, on_stop=on_stop_coro)
