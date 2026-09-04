"""
IngestionQueueManager - Manages, autosaves, and adds to the queue of resources to ingest.
"""

import threading
import os
import time

from centipede.internal.job import Job

# How long a caller waits for work before being handed nothing, when it does not
# ask for a particular wait.
DEFAULT_IDLE_WAIT_SECONDS = 1.0

# The shortest the queue will sleep for. Small enough to be imperceptible, large
# enough that a wait which computes to zero cannot become a busy loop.
MINIMUM_SLEEP_SECONDS = 0.005


class IngestionQueueManager(object):

    def __init__(self, config=None):
        # A list of urls that are in queue to be scraped.
        self.config = config
        self.queue_lock = threading.Lock()
        # Set whenever work is added, so a waiting caller is handed a new resource
        # as soon as it arrives instead of at the end of its sleep.
        self.work_available = threading.Event()
        self.periodic_queue = []
        self.immediate_queue = []
        # No longer load from autosave

        self.autosave_thread = None
        self.batch_ingest_thread = None

        self.is_periodic = config["periodic"]
        self.period_seconds = config.get("period_seconds", 0)

        # Whether a seed runs as soon as the pipeline starts, or waits out a full
        # period first. A seed is on the queue because it is wanted, so the default
        # is to fetch it now and let the repeats keep the schedule.
        self.run_at_startup = config.get("run_at_startup", True)

        self.idle_wait_seconds = config.get("idle_wait_seconds", DEFAULT_IDLE_WAIT_SECONDS)

        for seed in config["seed_urls"]:
            self.periodic_queue.append(self._job_for_seed(seed))

        self.throttled = config["throttled"]
        self.last_job_time = -1
        self.throttle_period_seconds = None
        if self.throttled:
            self.throttle_period_seconds = config["throttle_period_seconds"]


    def _job_for_seed(self, seed):
        """
        Builds the job for one entry of seed_urls.

        A seed is either the resource itself, or a dictionary carrying it
        alongside the settings that apply to that seed alone:

            {"url": "https://example.com/", "period_seconds": 3600}

        Anything the dictionary leaves out falls back to the pipeline-wide value.
        """
        if isinstance(seed, dict):
            data_point = seed["url"]
            period_seconds = seed.get("period_seconds", self.period_seconds)
            run_at_startup = seed.get("run_at_startup", self.run_at_startup)
        else:
            data_point = seed
            period_seconds = self.period_seconds
            run_at_startup = self.run_at_startup

        new_job = Job(data_point, self.is_periodic, period_seconds=period_seconds)
        if run_at_startup:
            new_job.make_due_now()
        return new_job


    def _load_autosave(self):
        base_dir = self.config["INGESTION_QUEUE_AUTOSAVE_BASE_DIR"]
        autosave_filename = os.path.join(base_dir, "ingestion_queue.txt")

        fp = open(autosave_filename, "r")
        self.queue_lock.acquire()
        ingestion_queue = fp.readlines()
        self.queue_lock.release()
        fp.close()

        return ingestion_queue

    def has_next(self):
        return len(self.periodic_queue) > 0 or len(self.immediate_queue) > 0

    def next_resource(self, timeout=0):
        """
        Returns the next resource to be consumed, or None if none comes up
        within timeout seconds.

        The default timeout of zero looks once and returns straight away. Give it
        a timeout to wait for the next job instead of asking again in a loop.
        """
        deadline = time.time() + timeout

        while True:
            next_job = self._take_due_job()
            if next_job:
                return next_job

            remaining = deadline - time.time()
            if remaining <= 0:
                return None

            # Sleep until the next job falls due or the throttle lifts, whichever
            # is later, and no further than the caller asked to wait for. A push
            # onto the queue sets the event and cuts the sleep short.
            sleep_for = max(MINIMUM_SLEEP_SECONDS, min(remaining, self._seconds_until_ready()))
            self.work_available.wait(sleep_for)
            self.work_available.clear()


    def _take_due_job(self):
        """
        Pops the job that should run now, or None if none should yet.
        """
        if self.throttled and time.time() - self.last_job_time <= self.throttle_period_seconds:
            return None

        with self.queue_lock:
            if len(self.immediate_queue) > 0:
                next_job = self.immediate_queue.pop(0)

            else:
                # Seeds keep their own periods, so the queue order is not the due
                # order: take the job that came due first, not the one in front.
                due_jobs = [job for job in self.periodic_queue if job.is_ready()]
                if not due_jobs:
                    return None

                next_job = min(due_jobs, key=lambda job: job.due_time)
                self.periodic_queue.remove(next_job)

                if next_job.repeat:
                    self.periodic_queue.append(
                        Job(next_job.data_point, next_job.repeat, period_seconds=next_job.period)
                    )

        self.last_job_time = time.time()
        return next_job


    def _seconds_until_ready(self):
        """
        How long before a job could come off the queue, ignoring any that arrive
        in the meantime.
        """
        now = time.time()

        throttle_wait = 0.0
        if self.throttled:
            throttle_wait = max(0.0, (self.last_job_time + self.throttle_period_seconds) - now)

        with self.queue_lock:
            if len(self.immediate_queue) > 0:
                return throttle_wait
            due_times = [job.due_time for job in self.periodic_queue]

        if not due_times:
            # Nothing queued at all, so the only thing that can change is a push,
            # and that wakes the sleeper itself.
            return self.idle_wait_seconds

        return max(throttle_wait, max(0.0, min(due_times) - now))


    def push_data_point(self, url):
        """
        Adds a resource to the ingestion queue.
        """
        self.queue_lock.acquire()
        new_job = Job(url, False)
        self.immediate_queue.append(new_job)
        self.queue_lock.release()
        self.work_available.set()


    def push_resources(self, resources):
        """
        Appends multiple new resources to the ingestion queue
        """
        new_jobs = []
        for resource in resources:
            new_jobs.append(Job(resource, False))

        self.queue_lock.acquire()
        self.immediate_queue.extend(new_jobs)
        self.queue_lock.release()
        self.work_available.set()


    def _autosave_queue(self):
        """
        Saves the entire ingestion_queue to a file
        """
        base_dir = self.config.INGESTION_QUEUE_AUTOSAVE_BASE_DIR
        autosave_filename = os.path.join(base_dir, "ingestion_queue.txt")

        fp = open(autosave_filename, "w+")
        self.queue_lock.acquire()
        fp.write(self.immediate_queue.join("\n"))
        self.queue_lock.release()
        fp.close()


    def _ingest_from_file(self):
        """
        Adds all resources described in a folder to the ingestion queue.
        """
        ingestion_input_base_dir = self.config.INGESTION_QUEUE_INPUT_DIR
        for filename in os.listdir(ingestion_input_base_dir):
            full_filepath = os.path.join(ingestion_input_base_dir, filename)
            fp = open(full_filepath, "r")
            resources = fp.readlines()
            fp.close()

            os.remove(full_filepath)

            self.queue_lock.acquire()
            self.immediate_queue.extend(resources)
            self.queue_lock.release()


    def start_periodic_tasks(self):
        """
        Creates and starts the autosave and batch ingestion threads
        """
        autosave_thread = threading.Timer(10.0, self._autosave_queue)
        batch_ingest_thread = threading.Timer(10.0, self._ingest_from_file)

        autosave_thread.start()
        batch_ingest_thread.start()
