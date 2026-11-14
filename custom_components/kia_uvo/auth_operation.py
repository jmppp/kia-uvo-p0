"""One auth flight: thread-safe start boundary, loop-owned result and completion."""
from threading import Lock


class AuthOperation:
    def __init__(self, loop, completion):
        self.loop = loop
        self.completion = completion
        self.result = loop.create_future()
        self._lock = Lock()
        self._state = "queued"
        self._completion_claimed = False

    @property
    def state(self):
        with self._lock:
            return self._state

    @property
    def busy(self):
        return self.state != "terminal" or not self.result.done()

    def begin(self):
        with self._lock:
            if self._state != "queued":
                return False
            self._state = "running"
            return True

    def _finish(self, expected, error):
        with self._lock:
            if self._state != expected:
                return False
            self._state = "terminal"
        try:
            self.loop.call_soon_threadsafe(self.completion, self, error)
        except RuntimeError:
            # A closed process loop cannot persist tokens or admit more work.
            pass
        return True

    def abort_queued(self, error):
        return self._finish("queued", error)

    def finish_worker(self, error):
        return self._finish("running", error)

    def claim_completion(self):
        with self._lock:
            if self._state != "terminal" or self._completion_claimed:
                return False
            self._completion_claimed = True
            return True
