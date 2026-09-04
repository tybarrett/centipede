import time


class Job(object):
    """
    One resource to ingest, and the time at which it becomes due.

    A job carries its own due time rather than deriving one from a single
    pipeline-wide period, so seeds on different schedules can share a queue.
    """

    def __init__(self, data_point, repeat, period_seconds=0, due_time=None):
        self.init_time = time.time()
        self.data_point = data_point
        self.period = period_seconds
        self.repeat = repeat
        self.scheduled_time = None
        self.due_time = self.init_time + self.period if due_time is None else due_time

    def set_period(self, period_seconds):
        """
        Sets how long to wait between repeats, and moves the due time with it.
        """
        self.period = period_seconds
        self.due_time = self.init_time + period_seconds

    def make_due_now(self):
        """
        Brings the job forward so that it runs on the next check.
        """
        self.due_time = time.time()

    def schedule_job(self, datetime_obj):
        """
        Sets the job to run at a specific time rather than after a period.
        """
        self.scheduled_time = datetime_obj
        self.due_time = datetime_obj.timestamp()

    def seconds_until_due(self):
        """
        How long until is_ready() turns True. Zero once the job is due.
        """
        return max(0.0, self.due_time - time.time())

    def is_ready(self):
        return time.time() >= self.due_time
