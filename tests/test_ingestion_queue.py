"""What comes off the ingestion queue, and when."""

import threading
import time

from centipede.internal.ingestion_queue_manager import IngestionQueueManager


def make_queue(**overrides):
    config = {
        "periodic": True,
        "period_seconds": 3600,
        "seed_urls": [],
        "throttled": False,
    }
    config.update(overrides)
    return IngestionQueueManager(config=config)


def drain(queue, count, timeout=2.0):
    """The next `count` data points, in the order the queue hands them over."""
    taken = []
    for _ in range(count):
        job = queue.next_resource(timeout=timeout)
        taken.append(job.data_point if job else None)
    return taken


class TestStartup:
    def test_a_seed_is_ready_at_startup(self):
        queue = make_queue(seed_urls=["https://example.com/"])

        job = queue.next_resource()

        assert job is not None
        assert job.data_point == "https://example.com/"

    def test_a_seed_can_be_made_to_wait_out_its_period_first(self):
        queue = make_queue(seed_urls=["https://example.com/"], run_at_startup=False)

        assert queue.next_resource() is None

    def test_a_repeat_waits_its_period_rather_than_running_again_at_once(self):
        queue = make_queue(seed_urls=["https://example.com/"], period_seconds=0.15)

        assert queue.next_resource() is not None
        assert queue.next_resource() is None, "the repeat did not wait for its period"

        time.sleep(0.16)
        assert queue.next_resource() is not None


class TestPerSeedSettings:
    def test_a_seed_can_carry_its_own_period(self):
        queue = make_queue(
            period_seconds=3600,
            seed_urls=[
                "https://slow.example/",
                {"url": "https://fast.example/", "period_seconds": 0.1},
            ],
        )

        assert sorted(drain(queue, 2)) == ["https://fast.example/", "https://slow.example/"]

        # Only the seed with the short period comes round again.
        time.sleep(0.12)
        assert drain(queue, 1) == ["https://fast.example/"]

    def test_a_seed_can_opt_out_of_running_at_startup(self):
        queue = make_queue(
            seed_urls=[
                "https://now.example/",
                {"url": "https://later.example/", "run_at_startup": False},
            ],
        )

        assert queue.next_resource().data_point == "https://now.example/"
        assert queue.next_resource() is None

    def test_a_seed_dictionary_falls_back_to_the_pipeline_settings(self):
        queue = make_queue(period_seconds=0.1, seed_urls=[{"url": "https://example.com/"}])

        assert queue.next_resource() is not None
        time.sleep(0.11)
        assert queue.next_resource() is not None

    def test_the_job_that_came_due_first_is_taken_first(self):
        """Seeds on different periods do not come due in queue order."""
        queue = make_queue(
            periodic=False,
            run_at_startup=False,
            seed_urls=[
                {"url": "https://third.example/", "period_seconds": 0.3},
                {"url": "https://first.example/", "period_seconds": 0.1},
                {"url": "https://second.example/", "period_seconds": 0.2},
            ],
        )

        assert drain(queue, 3) == [
            "https://first.example/",
            "https://second.example/",
            "https://third.example/",
        ]

    def test_a_repeat_rejoins_the_queue_in_due_order(self):
        """A seed on a short period cycles while one on a long period waits."""
        queue = make_queue(
            run_at_startup=False,
            seed_urls=[
                {"url": "https://slow.example/", "period_seconds": 0.3},
                {"url": "https://quick.example/", "period_seconds": 0.1},
            ],
        )

        assert drain(queue, 3) == [
            "https://quick.example/",
            "https://quick.example/",
            "https://slow.example/",
        ]


class TestWaiting:
    def test_the_default_call_does_not_wait(self):
        queue = make_queue(seed_urls=["https://example.com/"], run_at_startup=False)

        started = time.time()
        assert queue.next_resource() is None
        assert time.time() - started < 0.05

    def test_a_timeout_waits_for_a_job_that_is_not_due_yet(self):
        queue = make_queue(seed_urls=["https://example.com/"], run_at_startup=False,
                           period_seconds=0.2)

        started = time.time()
        job = queue.next_resource(timeout=2.0)
        waited = time.time() - started

        assert job is not None
        assert 0.15 < waited < 1.0, "waited %.3fs" % waited

    def test_a_timeout_gives_up_and_returns_nothing(self):
        queue = make_queue(seed_urls=["https://example.com/"], run_at_startup=False)

        started = time.time()
        assert queue.next_resource(timeout=0.2) is None
        assert 0.15 < time.time() - started < 1.0

    def test_a_pushed_resource_wakes_a_waiting_caller(self):
        """A wait ends when work arrives, not when the sleep happens to run out."""
        queue = make_queue(seed_urls=[])
        threading.Timer(0.1, queue.push_data_point, args=("https://pushed.example/",)).start()

        started = time.time()
        job = queue.next_resource(timeout=10.0)
        waited = time.time() - started

        assert job is not None and job.data_point == "https://pushed.example/"
        assert waited < 1.0, "took %.3fs to notice a pushed resource" % waited

    def test_waiting_does_not_burn_the_processor(self):
        queue = make_queue(seed_urls=[])

        started_cpu = time.process_time()
        queue.next_resource(timeout=0.5)
        used_cpu = time.process_time() - started_cpu

        assert used_cpu < 0.05, "used %.3fs of cpu waiting half a second" % used_cpu


class TestThrottle:
    def test_jobs_are_spaced_by_the_throttle(self):
        queue = make_queue(
            seed_urls=["https://a.example/", "https://b.example/"],
            throttled=True,
            throttle_period_seconds=0.2,
        )

        assert queue.next_resource() is not None
        assert queue.next_resource() is None, "the throttle let a second job straight through"

        started = time.time()
        assert queue.next_resource(timeout=2.0) is not None
        assert time.time() - started > 0.1

    def test_a_throttled_wait_does_not_burn_the_processor(self):
        queue = make_queue(
            seed_urls=["https://a.example/", "https://b.example/"],
            throttled=True,
            throttle_period_seconds=5.0,
        )
        queue.next_resource()

        started_cpu = time.process_time()
        queue.next_resource(timeout=0.5)
        used_cpu = time.process_time() - started_cpu

        assert used_cpu < 0.05, "used %.3fs of cpu waiting on the throttle" % used_cpu


class TestPushedResources:
    def test_pushed_resources_come_before_periodic_ones(self):
        queue = make_queue(seed_urls=["https://seed.example/"], run_at_startup=False)
        queue.push_resources(["https://one.example/", "https://two.example/"])

        assert drain(queue, 2) == ["https://one.example/", "https://two.example/"]

    def test_a_pushed_resource_runs_once_and_does_not_repeat(self):
        queue = make_queue(seed_urls=[])
        queue.push_data_point("https://once.example/")

        assert queue.next_resource() is not None
        assert queue.next_resource() is None
