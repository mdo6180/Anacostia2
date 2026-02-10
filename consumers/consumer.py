from logging import Logger
import threading
import queue
from typing import Callable, Any, Optional, List
from logging import Logger

from connection import ConnectionManager
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
        self.filter_func = filter_func

        self.items_queue = queue.Queue(maxsize=maxsize)
        self._stop = threading.Event()
        self._thread = None

        self.logger = logger
        self.conn_manager: ConnectionManager = None
        self.global_usage_table_name = "artifact_usage_events"

        self.run_id = 0

    def set_db_path(self, db_path: str):
        self.db_path = db_path
    
    def set_run_id(self, run_id: int):
        self.run_id = run_id

    def ignore_artifact(self, artifact_hash: str) -> None:
        # delete this query in future if we don't need to store file paths for ignored artifacts
        filepath = self.stream.get_artifact_path(artifact_hash)

        with self.conn_manager.write_cursor() as cursor:
            cursor.execute(
                f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} (artifact_hash, node_name, run_id, state, details)
                VALUES (?, ?, ?, ?, ?);
                """,
                (artifact_hash, self.name, self.run_id, "ignored", filepath)
            )

    def start(self):
        def run():
            self.conn_manager = ConnectionManager(db_path=self.db_path, logger=self.logger)

            bundle_items: List[Any] = []
            bundle_hashes: List[str] = []

            for item, file_hash in self.stream:
                if self._stop.is_set():
                    break

                # Apply filtering function if provided
                if self.filter_func is not None:
                    if not self.filter_func(item):
                        self.logger.info(f"{self.name} ignore_artifact: {item}")       # ignore_artifact DB call in future
                        self.ignore_artifact(file_hash)    # mark artifact as ignored in the DB
                        continue

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

        # it might be better to get the last bundle by checking which items are marked as "using"

        while not self._stop.is_set():
            bundle_items, bundle_hashes = self.items_queue.get(block=True)
            self.bundle_hashes = bundle_hashes  # store the hashes of the current bundle for the using_artifacts and commit_artifacts calls in the Node
            yield bundle_items