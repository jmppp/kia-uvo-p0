"""Opt-in, per-worker transport gate. No retries or network of its own."""
from contextlib import contextmanager
from contextvars import ContextVar
from time import monotonic
from .exceptions import APIError

class CommandExpired(APIError):
    """Not authorized to enter another transport send."""

_current = ContextVar("kia_command_gate", default=None)

class CommandGate:
    def __init__(self, authorize, deadline):
        self.authorize = authorize
        self.deadline = deadline
        self.entered = False

    def check(self):
        if self.entered or monotonic() >= self.deadline:
            raise CommandExpired("Command expired or transport already entered")

    def before_send(self):
        self.check()
        self.authorize()  # synchronously ask the HA event loop, after executor wait
        self.check()      # includes time spent waiting for that answer
        self.entered = True  # conservative: unknown POST outcome consumes the lease

@contextmanager
def command_scope(gate):
    token = _current.set(gate)
    try:
        yield
    finally:
        _current.reset(token)

def before_send():
    if (gate := _current.get()) is not None:
        gate.before_send()

def before_retry():
    if (gate := _current.get()) is not None:
        gate.check()  # never re-register/re-send a consumed control request
