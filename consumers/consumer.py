from logging import Logger
import threading
import queue
from typing import Callable, Any, Optional, List, Tuple
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
        """
        Note: you can only use one Consumer object for one node. Consumer objects cannot be shared between nodes. 
        """

        if bundle_size <= 0:
            raise ValueError("bundle_size must be >= 1")

        self.name = name
        self.stream = stream
        self.bundle_size = bundle_size
        self.filter_func = filter_func

        self.bundle_items: List[Any] = []  # store the items of the current bundle for the using_artifacts and commit_artifacts calls in the Node
        self.bundle_hashes: List[str] = []  # store the hashes of the current bundle for the using_artifacts and commit_artifacts calls in the Node

        self.items_queue = queue.Queue(maxsize=maxsize)
        self._stop = threading.Event()
        self._thread = None

        self.logger = logger
        self.conn_manager: ConnectionManager = None
        self.global_usage_table_name = "artifact_usage_events"

        self.run_id = 0

        # restart mode, 0 means no restart, 1 means restart from the beginning of the last run, 2 means restart from new run and process primed but unused artifacts
        self.restart = 0
        self.node_name = None
    
    def set_node_name(self, node_name: str):
        if self.node_name is not None:
            raise ValueError(f"This Consumer object has already been assigned to node {self.node_name}. Consumer objects cannot be shared between nodes.")
        self.node_name = node_name

    def set_restart_mode(self, mode: int):
        """
        mode: int = 1 or 2.
        1 means restart from the beginning of the last run, i.e., re-process all artifacts in the last run marked as "using" in the DB. 
        This is useful when you want to re-process the same artifacts again after fixing an issue in the processing logic.
        2 means restart from new run, e.g., run 1 has finished, 
        restart by starting run 2 and process artifacts that have been primed but have not been marked as using in the DB.
        If there are not enough primed artifacts to form a full bundle, wait for new artifacts to be primed until a full bundle can be formed, 
        then yield that bundle and continue processing new artifacts from the stream.
        """
        self.restart = mode

    def set_db_path(self, db_path: str):
        self.db_path = db_path
    
    def set_run_id(self, run_id: int):
        self.run_id = run_id

    def prime_artifact(self, artifact_hash: str) -> None:
        # delete this query in future if we don't need to store file paths for ignored artifacts
        filepath = self.stream.get_artifact_path(artifact_hash)

        with self.conn_manager.write_cursor() as cursor:
            cursor.execute(
                f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} (artifact_hash, node_name, state, details)
                VALUES (?, ?, ?, ?);
                """,
                (artifact_hash, self.name, "primed", filepath)
            )
    
    def ignore_artifact(self, artifact_hash: str) -> None:
        # delete this query in future if we don't need to store file paths for ignored artifacts
        filepath = self.stream.get_artifact_path(artifact_hash)

        with self.conn_manager.write_cursor() as cursor:
            cursor.execute(
                f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} (artifact_hash, node_name, state, details)
                VALUES (?, ?, ?, ?);
                """,
                (artifact_hash, self.name, "ignored", filepath)
            )
    
    def is_artifact_used(self, artifact_hash: str) -> bool:
        with self.conn_manager.read_cursor() as cursor:
            cursor.execute(
                f"""
                SELECT COUNT(*) FROM {self.global_usage_table_name}
                WHERE node_name = ? AND artifact_hash = ? AND state = 'using';
                """,
                (self.node_name, artifact_hash)
            )
            result = cursor.fetchone()
            return result[0] > 0   # returns True if count > 0, else False

    def start(self):
        def run():
            self.conn_manager = ConnectionManager(db_path=self.db_path, logger=self.logger)

            for item, file_hash in self.stream:
                if self._stop.is_set():
                    break

                # Apply filtering function if provided
                if self.filter_func is not None:
                    if not self.filter_func(item):
                        # self.logger.info(f"{self.name} ignore_artifact: {item}")       # ignore_artifact DB call in future
                        self.ignore_artifact(file_hash)    # mark artifact as ignored in the DB
                        continue

                    else:
                        # self.logger.info(f"{self.name} prime_artifact: {item}")        # prime_artifact DB call in future
                        self.prime_artifact(file_hash)     # mark artifact as primed in the DB

                self.items_queue.put((item, file_hash), block=True)        # backpressure here, blocks if queue is full
                # self.logger.info(f"item: '{item}', file_hash: '{file_hash}' put in queue by {self.name}")

            # Optional: decide whether to flush partial batch on stop.
            # Current behavior: do NOT flush partial batch.

        self._thread = threading.Thread(name=self.name, target=run, daemon=True)
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()

    def get_using_artifacts(self) -> List[Tuple[Any, str]]:
        using_artifacts = []
        with self.conn_manager.read_cursor() as cursor:
            cursor.execute(
                f"""
                SELECT artifact_hash FROM {self.global_usage_table_name}
                WHERE node_name = ? AND run_id = ? AND state = 'using' AND artifact_hash IN (
                    SELECT artifact_hash FROM {self.stream.local_table_name}
                );
                """,
                (self.node_name, self.run_id)
            )
            using_artifacts = cursor.fetchall()
            self.logger.info(f"querying using {self.node_name} run {self.run_id}: found {len(using_artifacts)} artifacts in 'using' state")
        
        artifact_hashes = [row[0] for row in using_artifacts]   # extract artifact hashes from query result
        self.bundle_hashes = artifact_hashes  # store the hashes of the current using artifacts for the using_artifacts and commit_artifacts calls in the Node

        using_bundle = []
        for artifact_hash in artifact_hashes:
            content = self.stream.load_artifact(artifact_hash)
            using_bundle.append(content)
            
        return using_bundle
    
    def get_unused_artifacts(self) -> List[Tuple[Any, str]]:
        unused_artifacts = []
        with self.conn_manager.read_cursor() as cursor:
            cursor.execute(
                f"""
                SELECT artifact_hash FROM {self.global_usage_table_name}
                WHERE node_name = ? AND state = 'primed' AND artifact_hash NOT IN (
                    SELECT artifact_hash FROM {self.global_usage_table_name}
                    WHERE node_name = ? AND state = 'using'
                );
                """,
                (self.name, self.node_name,)
            )
            unused_artifacts = cursor.fetchall()
            # self.logger.info(f"querying primed {self.node_name} run {self.run_id}: found {len(unused_artifacts)} artifacts in 'primed' state")
        
        artifact_hashes = [row[0] for row in unused_artifacts]   # extract artifact hashes from query result
        self.bundle_hashes = artifact_hashes  # store the hashes of the current unused artifacts for the using_artifacts and commit_artifacts calls in the Node

        for artifact_hash in artifact_hashes:
            content = self.stream.load_artifact(artifact_hash)
            self.bundle_items.append(content)
            
    def __iter__(self):
        while not self._stop.is_set():
            if self.restart == 1:
                if self.conn_manager is None:
                    self.conn_manager = ConnectionManager(db_path=self.db_path, logger=self.logger)
                
                # retrieve artifacts that were primed but not marked as using, and yield those as well 
                # this can happen if the pipeline was stopped after priming artifacts but before starting to use them 
                # e.g., if the stop_if was triggered between runs before the using_artifacts call in the Node
                self.get_unused_artifacts()

            elif self.restart == 2:
                if self.conn_manager is None:
                    self.conn_manager = ConnectionManager(db_path=self.db_path, logger=self.logger)
                
                using_bundle = self.get_using_artifacts()
                if using_bundle:
                    self.logger.info(f"{self.name} yielding {len(using_bundle)} artifacts from last partial bundle after restart")
                    yield using_bundle
                    self.bundle_items = []
                    self.bundle_hashes = []

            self.restart = 0   # reset restart mode after restart is done

            if len(self.bundle_hashes) < self.bundle_size:
                item, file_hash = self.items_queue.get(block=True)

                # avoid adding duplicate artifacts to the bundle in case the same artifact is put in the queue multiple times due to restarts
                if self.is_artifact_used(file_hash) is False:   
                    self.bundle_items.append(item)
                    self.bundle_hashes.append(file_hash)
            else:
                self.logger.info(f"{self.name} yielding bundle_items: {self.bundle_items}, bundle_hashes: {self.bundle_hashes}")
                yield self.bundle_items
                self.bundle_items = []
                self.bundle_hashes = []
