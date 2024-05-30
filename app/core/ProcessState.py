from app.core import utils


class ProcessState:
    REQUESTED = 'requested'
    STARTED = 'started'
    RUNNING = 'running'
    PAUSED = 'paused'
    CANCELED = 'stopped'
    ERROR = 'error'
    FINISHED = 'finished'

    def __init__(self):
        self._ping = -1  # UTC ping time
        self._state = None
        self._progress = -1

    def get_state(self):
        return self._state

    def set_progress(self, pct):
        self._progress = pct

    def get_progress(self):
        return self._progress

    def set_ping_time(self):
        self._ping = utils.get_utc()

    def is_running(self):
        return (self._state == self.RUNNING)

    def set_running(self):
        self._state = self.RUNNING

    def is_paused(self):
        return (self._state == self.PAUSED)

    def set_paused(self):
        self._state = self.PAUSED

    def is_dead(self):
        return (self._state == self.ERROR)

    def set_dead(self):
        self._state = self.ERROR

    # 'canceled' is when the user explicitly stops the
    # process, as opposed to it finishing naturally
    def is_canceled(self):
        return (self._state == self.CANCELED)

    def set_canceled(self):
        self._state = self.CANCELED
