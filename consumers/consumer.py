from logging import Logger
import threading
import queue
from typing import Callable, Any, Optional, List

from logging import Logger
from utils.connection import ConnectionManager

from streams.directory import DirectoryStream



class Consumer:
    def __init__(
        self,
        name: str,
        stream: DirectoryStream,
        bundle_size: int = 1,
        maxsize: int = 0,
        filter_func: Optional[Callable[[Any], bool]] = None,
        logger: Logger = None
    ):
        if bundle_size <= 0:
            raise ValueError("bundle_size must be >= 1")

        self.name = name
        self.stream = stream
        self.bundle_size = bundle_size
        self.db_connection = None                           # placeholder for DB connection
        self.stream.set_db_connection(self.db_connection)   # set to actual DB connection in the future
        self.filter_func = filter_func

        self.items_queue = queue.Queue(maxsize=maxsize)
        self._stop = threading.Event()
        self._thread = None

        self.logger = logger

    def initialize_db_connection(self, filename: str):
        self.conn_manager = ConnectionManager(db_path=filename, logger=self.logger)

    def start(self):
        def run():
            bundle_items: List[Any] = []
            bundle_hashes: List[str] = []

            for item, file_hash in self.stream:
                if self._stop.is_set():
                    break

                # Apply filtering function if provided
                if self.filter_func is not None:
                    if not self.filter_func(item):
                        self.logger.info(f"{self.name} ignore_artifact: {item}")       # ignore_artifact DB call in future
                        continue
                    else:
                        self.logger.info(f"{self.name} prime_artifact: {item}")        # prime_artifact DB call in future

                # Item accepted (or no filter)
                bundle_items.append(item)
                bundle_hashes.append(file_hash)

                # Emit only when batch is full
                if len(bundle_items) >= self.bundle_size:
                    self.items_queue.put((bundle_items, bundle_hashes), block=True)        # backpressure here, blocks if queue is full
                    bundle_items = []
                    bundle_hashes = []

            # Optional: decide whether to flush partial batch on stop.
            # Current behavior: do NOT flush partial batch.

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()

    def __iter__(self):
        # get the last partial bundle by checking which items are primed but not yet in the use_artifact state
        # yield the last bundle when the StreamRunner is restarted here

        while not self._stop.is_set():
            bundle_items, bundle_hashes = self.items_queue.get(block=True)
            self.bundle_hashes = bundle_hashes  # store the hashes of the current bundle for the using_artifacts and commit_artifacts calls in the Node
            yield bundle_items