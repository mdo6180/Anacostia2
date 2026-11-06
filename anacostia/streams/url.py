from logging import Logger
from collections.abc import Iterator
import time

from anacostia.streams.base import Stream
from anacostia.utils.types import Artifact



class URLEndpointStream(Stream):
    def __init__(self, name: str, url: str, poll_interval: float = 0.1, hash_chunk_size: int = 1_048_576, logger: Logger = None):
        """
        Initialize a URLEndpointStream instance.

        :param name: Name of the stream.
        :param url: The URL to monitor.
        :param poll_interval: The interval (in seconds) at which the stream polls the endpoint for new artifacts.
        :param hash_chunk_size: The size of chunks to read when hashing responses from the endpoint.
        :param logger: Logger instance for logging.
        """
        super().__init__(name=name, source=url, poll_interval=poll_interval, logger=logger)

        self.name = name
        self.url = url
        self.poll_interval = poll_interval
        self.hash_chunk_size = hash_chunk_size
        self.logger = logger

    def __iter__(self) -> Iterator[Artifact]:
        """
        Poll the endpoint for new artifacts, register them in the database,
        and yield newly discovered artifacts in chronological order.

        Implementation Note: This method is a placeholder and should be implemented to handle the specific logic for polling the URL endpoint, 
        processing the response, and registering artifacts. 
        The current implementation simply sleeps for the specified poll interval to prevent blocking the main thread.
        """

        while True:
            # Do something here

            # IMPORTANT: prevent polling from blocking the main thread.
            time.sleep(self.poll_interval)