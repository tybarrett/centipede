"""When a job is due."""

import time
from datetime import datetime, timedelta

from centipede.internal.job import Job


def test_a_job_with_a_period_is_due_once_that_period_has_passed():
    job = Job("https://example.com/", True, period_seconds=0.05)

    assert not job.is_ready()
    time.sleep(0.06)
    assert job.is_ready()


def test_a_job_with_no_period_is_due_immediately():
    assert Job("https://example.com/", False).is_ready()


def test_make_due_now_brings_a_waiting_job_forward():
    job = Job("https://example.com/", True, period_seconds=3600)
    assert not job.is_ready()

    job.make_due_now()

    assert job.is_ready()


def test_set_period_moves_the_due_time_with_it():
    job = Job("https://example.com/", True, period_seconds=3600)

    job.set_period(0.05)

    assert job.period == 0.05
    assert not job.is_ready()
    time.sleep(0.06)
    assert job.is_ready()


def test_schedule_job_sets_the_due_time_to_that_moment():
    job = Job("https://example.com/", False)

    job.schedule_job(datetime.now() + timedelta(hours=1))

    assert not job.is_ready()
    assert 3400 < job.seconds_until_due() <= 3600


def test_seconds_until_due_is_zero_once_a_job_is_due():
    assert Job("https://example.com/", False).seconds_until_due() == 0.0
