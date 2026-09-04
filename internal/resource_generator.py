
import logging

from centipede.internal import centipede_logger, ingestion_queue_manager


class UrlGenerator(object):

    def __init__(self, config=None):
        self.resource_queue = ingestion_queue_manager.IngestionQueueManager(config)

        self.logger = centipede_logger.create_logger(self.__class__.__name__, logging.DEBUG)


    def iterate_pages(self, idle_wait_seconds=None):
        """
        Yields jobs as they come due, waiting for the next one in between.

        Yields None when a wait passes with nothing due, so a caller can do its
        own periodic work between resources. How long that wait is comes from
        idle_wait_seconds, or from the queue's own setting when not given.
        """
        if idle_wait_seconds is None:
            idle_wait_seconds = self.resource_queue.idle_wait_seconds

        while True:
            yield self.resource_queue.next_resource(timeout=idle_wait_seconds)


    def add_to_queue(self, resources):
        self.resource_queue.push_resources(resources)
