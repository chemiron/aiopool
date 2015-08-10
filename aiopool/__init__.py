import asyncio
import importlib
import logging

from .base import IDLE_CHECK, IDLE_TIME
from .fork import ForkWorker
from .spawn import SpawnWorker


__all__ = ['fork', 'spawn']

logger = logging.getLogger(__name__)


class _subprocess:

    idle_time = IDLE_TIME
    idle_check = IDLE_CHECK

    start_args = start_kwargs = stop_arg = None
    _on_start = _on_stop = None

    def __init__(self, on_start=None, on_stop=None):
        self._on_start = on_start
        self._on_stop = on_stop

    def on_start(self, fn):
        self._on_start = fn
        return self

    def on_stop(self, fn):
        self._on_stop = fn
        return self

    @asyncio.coroutine
    def call_on_start(self):
        on_start_coro = self._on_start
        if on_start_coro is None:
            return
        if not asyncio.iscoroutinefunction(on_start_coro):
            on_start_coro = asyncio.coroutine(on_start_coro)
        self.stop_arg = result = yield from on_start_coro(
            *self.start_args, **self.start_kwargs)
        return result

    @asyncio.coroutine
    def call_on_stop(self):
        on_stop_coro = self._on_stop
        if on_stop_coro is None:
            return
        if not asyncio.iscoroutinefunction(on_stop_coro):
            on_stop_coro = asyncio.coroutine(on_stop_coro)
        return (yield from on_stop_coro(self.stop_arg))

    def __call__(self, *args, **kwargs):
        self.start_args = args
        self.start_kwargs = kwargs
        return self.get_worker()

    def get_worker(self):
        raise NotImplementedError


class fork(_subprocess):

    def get_worker(self):
        return ForkWorker(asyncio.get_event_loop(), self,
                          idle_time=self.idle_time,
                          idle_check=self.idle_check)


class spawn(_subprocess):

    def get_worker(self):
        return SpawnWorker(asyncio.get_event_loop(), self,
                           idle_time=self.idle_time,
                           idle_check=self.idle_check)

    def __getstate__(self):
        state = {'start_args': self.start_args,
                 'start_kwargs': self.start_kwargs}
        _on_start = self._on_start
        if _on_start is not None:
            state['_on_start'] = (_on_start.__module__,
                                  _on_start.__name__)
        _on_stop = self._on_stop
        if _on_stop is not None:
            state['_on_stop'] = (_on_stop.__module__,
                                 _on_stop.__name__)
        return state

    def __setstate__(self, state):
        self.start_args = state['start_args']
        self.start_kwargs = state['start_kwargs']

        methods = (state.get('_on_start'),
                   state.get('_on_stop'))
        for method in methods:
            if method is None:
                continue
            module, name = method
            module = importlib.import_module(module)
            # after the function is decorated by subprocess
            # the subprocess object will replace the function
            sub_obj = getattr(module, name, None)
            if sub_obj is not None:
                self._on_start = sub_obj._on_start
                self._on_stop = sub_obj._on_stop
                break



