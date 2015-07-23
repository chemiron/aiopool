import asyncio
import ctypes
import multiprocessing as mp
import time

from .base import WorkerProcess, ChildProcess


class _State(ctypes.Structure):

    _fields_ = [
        ('ping', ctypes.c_bool),
        ('pong', ctypes.c_bool),
        ('close', ctypes.c_bool)
    ]


def _worker(state, loader):
    """ It's a target method for subprocess.

        Defined as a separate function to prevent
        transmitting of the Worker to child process.
    """
    asyncio.set_event_loop(None)
    process = SpawnChild(state, loader)
    process.start()


class SpawnChild(ChildProcess):

    _heartbeat_task = None
    ping = None

    def __init__(self, state, loader):
        ChildProcess.__init__(self, loader)
        self.state = state

    @asyncio.coroutine
    def on_start(self):
        self.ping = time.monotonic()
        self._heartbeat_task = asyncio.Task(self.heartbeat())

    def stop(self):
        if self._heartbeat_task is not None:
            self._heartbeat_task.cancel()
        ChildProcess.stop(self)

    @asyncio.coroutine
    def heartbeat(self):
        while True:
            yield from asyncio.sleep(15)

            if self.state.ping:
                self.state.ping = False
                self.ping = time.monotonic()

            if self.state.close:
                self.state.close = False
                self.stop()
                break

            if (time.monotonic() - self.ping) < 30:
                self.state.pong = True
            else:
                self.stop()
                return


class SpawnWorker(WorkerProcess):

    SPAWN_METHOD = 'spawn'

    process = ping = None
    heartbeat_task = None

    def start_child(self):
        ctx = mp.get_context(self.SPAWN_METHOD)
        state = ctx.Value(_State, False, False, False)
        process = ctx.Process(target=_worker,
                              args=(state, self.loader))
        process.start()
        asyncio.async(self.connect(process, state))

    def kill_child(self):
        self.heartbeat_task.cancel()
        self.process.terminate()
        self.process.join()

    @asyncio.coroutine
    def heartbeat(self, state):
        while True:
            yield from asyncio.sleep(15)
            if state.pong:
                state.pong = False
                self.ping = time.monotonic()

            if (time.monotonic() - self.ping) < 30:
                state.ping = True
            else:
                self.restart()
                return

    @asyncio.coroutine
    def connect(self, process, state):
        # store info
        self.process = process
        self.ping = time.monotonic()
        self.heartbeat_task = asyncio.Task(self.heartbeat(state))
