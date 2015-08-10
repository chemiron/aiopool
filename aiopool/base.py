import asyncio
import os
import signal


# if it's idle will be restarted after N seconds
IDLE_TIME = 30
# check for idle each N seconds
IDLE_CHECK = 5


class ChildProcess:

    loop = None

    def __init__(self, loader, **options):
        self.loader = loader
        self.options = options

    def start(self):
        self.loop = loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

        asyncio.async(self.on_start())
        asyncio.async(self.loader.call_on_start())
        loop.add_signal_handler(signal.SIGINT, self.stop)
        loop.add_signal_handler(signal.SIGTERM, self.stop)

        loop.run_forever()
        loop.run_until_complete(self.loader.call_on_stop())

        loop.remove_signal_handler(signal.SIGINT)
        loop.remove_signal_handler(signal.SIGTERM)
        os._exit(0)

    @asyncio.coroutine
    def on_start(self):
        """ It's a callback which will be called when event loop starts. """

    def stop(self):
        if self.loop.is_running():
            self.loop.stop()


class WorkerProcess:

    _started = _restart = False

    def __init__(self, loop, loader, **options):
        self.loop = loop
        self.loader = loader
        self.options = options
        self.start()

    def start(self):
        assert not self._started
        self._started = True
        self.start_child()

    def kill(self):
        self._started = False
        self.kill_child()

    def restart(self):
        if not self._restart:
            self._restart = True
            self.kill()
            self.start()
            self._restart = False

    def start_child(self):
        raise NotImplementedError

    def kill_child(self):
        raise NotImplementedError
