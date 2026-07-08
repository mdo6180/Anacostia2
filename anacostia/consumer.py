from logging import Logger
import threading
import queue
from typing import Callable, Any, Optional, List, Tuple
from logging import Logger
import json

from anacostia.streams.base import Stream
from anacostia.utils.connection import ConnectionManager
from anacostia.utils.logging import log
from anacostia.utils.types import JsonDict, Artifact


sql = str   # alias of the str type for syntax highlighting using the Python Inline Source Syntax Highlighting extension by Sam Willis in VSCode.



class Consumer:
    def __init__(
        self,
        name: str,
        stream: Stream,
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
        self.stream: Stream = stream
        self.bundle_size = bundle_size
        self.filter_func = filter_func

        #self.bundle_locations: List[Any] = []  # store the items of the current bundle for the using_artifacts and commit_artifacts calls in the Node
        #self.bundle_hashes: List[str] = []  # store the hashes of the current bundle for the using_artifacts and commit_artifacts calls in the Node
        self.bundle_artifacts: List[Artifact] = []  # store the Artifact objects of the current bundle for the using_artifacts and commit_artifacts calls in the Node

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
        filepath = self.stream.get_artifact_location(artifact_hash)

        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} 
                (artifact_hash, node_name, state, details) 
                VALUES (?, ?, ?, ?);
            """
            cursor.execute(query, (artifact_hash, self.name, "primed", filepath))
        
    def record_provenance(self, run_id: int, details: str = None) -> None:
        # record edges between the stream and the consumer for all artifacts detected between the start of the current run and the previous run
        for artifact_location, artifact_hash in self.get_detected_artifacts(current_run_id=run_id):
            self.conn_manager.add_provenance_edge(
                predecessor_name=self.stream.name, predecessor_type="stream",
                successor_name=self.name, successor_type="consumer",
                artifact_location=artifact_location,
                artifact_hash=artifact_hash,
                run_id=run_id
            )

        # record edges between the consumer and the node for the artifacts in the current bundle
        for artifact in self.bundle_artifacts[:self.bundle_size]:
            self.conn_manager.add_provenance_edge(
                predecessor_name=self.name, predecessor_type="consumer",
                successor_name=self.node_name, successor_type="node",
                artifact_location=json.dumps(artifact.location),
                artifact_hash=artifact.hash,
                run_id=run_id
            )
        """
        for artifact_hash in self.bundle_hashes[:self.bundle_size]:
            self.conn_manager.add_provenance_edge(
                predecessor_name=self.name, predecessor_type="consumer",
                successor_name=self.node_name, successor_type="node",
                artifact_location=json.dumps(self.stream.get_artifact_location(artifact_hash)),
                artifact_hash=artifact_hash,
                run_id=run_id
            ) 
        """

    def ignore_artifact(self, artifact_hash: str) -> None:
        # delete this query in future if we don't need to store file paths for ignored artifacts
        artifact_location = self.stream.get_artifact_location(artifact_hash)

        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} 
                (artifact_hash, node_name, state, details) 
                VALUES (?, ?, ?, ?);
            """
            cursor.execute(query, (artifact_hash, self.name, "ignored", artifact_location))
    
    def is_artifact_used(self, artifact_hash: str) -> bool:
        with self.conn_manager.read_cursor() as cursor:
            query: sql = f"""
                SELECT COUNT(*) FROM {self.global_usage_table_name}
                WHERE node_name = ? AND artifact_hash = ? AND state = 'using';
            """
            cursor.execute(query, (self.node_name, artifact_hash))
            result = cursor.fetchone()
            return result[0] > 0   # returns True if count > 0, else False

    def start(self):
        def run():
            self.conn_manager = ConnectionManager(db_path=self.db_path, logger=self.logger)

            for artifact in self.stream:
                if self._stop.is_set():
                    break

                # Apply filtering function if provided
                if self.filter_func is not None:
                    if not self.filter_func(artifact):
                        # self.logger.info(f"{self.name} ignore_artifact: {item}")       # ignore_artifact DB call in future
                        self.ignore_artifact(artifact.hash)    # mark artifact as ignored in the DB
                        continue

                    else:
                        # self.logger.info(f"{self.name} prime_artifact: {item}")        # prime_artifact DB call in future
                        self.prime_artifact(artifact.hash)     # mark artifact as primed in the DB

                self.items_queue.put(artifact, block=True)        # backpressure here, blocks if queue is full
                # self.logger.info(f"item: '{item}', file_hash: '{file_hash}' put in queue by {self.name}")

            # Optional: decide whether to flush partial batch on stop.
            # Current behavior: do NOT flush partial batch.

        self._thread = threading.Thread(name=self.name, target=run, daemon=True)
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()

    def get_using_artifacts(self) -> List[Artifact]:
        using_artifacts = []
        with self.conn_manager.read_cursor() as cursor:
            query: sql = f"""
                SELECT artifact_hash FROM {self.global_usage_table_name}
                WHERE node_name = ? AND run_id = ? AND state = 'using' AND artifact_hash IN (
                    SELECT artifact_hash FROM {self.stream.local_table_name}
                )
                ORDER BY timestamp ASC;
            """
            cursor.execute(query, (self.node_name, self.run_id))
            using_artifacts = cursor.fetchall()
            log(f"querying using {self.node_name} run {self.run_id}: found {len(using_artifacts)} artifacts in 'using' state", level="info", logger=self.logger)
        
        artifact_hashes = [row[0] for row in using_artifacts]   # extract artifact hashes from query result
        #self.bundle_hashes = artifact_hashes  # store the hashes of the current using artifacts for the using_artifacts and commit_artifacts calls in the Node
        self.bundle_artifacts = [Artifact(location=self.stream.get_artifact_location(artifact_hash), hash=artifact_hash) for artifact_hash in artifact_hashes]

        """
        using_bundle = []
        for artifact_hash in artifact_hashes:
            artifact_location = self.stream.get_artifact_location(artifact_hash)
            #using_bundle.append(artifact_location)
            using_bundle.append(Artifact(location=artifact_location, hash=artifact_hash))
            
        return using_bundle
        """
        return self.bundle_artifacts
    
    def get_unused_artifacts(self) -> List[Artifact]:
        unused_artifacts = []
        with self.conn_manager.read_cursor() as cursor:
            query: sql = f"""
                SELECT artifact_hash FROM {self.global_usage_table_name}
                WHERE node_name = ? AND state = 'primed' AND artifact_hash NOT IN (
                    SELECT artifact_hash FROM {self.global_usage_table_name}
                    WHERE node_name = ? AND state = 'using'
                )
                ORDER BY timestamp ASC;
            """
            cursor.execute(query, (self.name, self.node_name,))
            unused_artifacts = cursor.fetchall()
            # self.logger.info(f"querying primed {self.node_name} run {self.run_id}: found {len(unused_artifacts)} artifacts in 'primed' state")
        
        artifact_hashes = [row[0] for row in unused_artifacts]   # extract artifact hashes from query result
        #self.bundle_hashes = artifact_hashes  # store the hashes of the current unused artifacts for the using_artifacts and commit_artifacts calls in the Node

        for artifact_hash in artifact_hashes:
            artifact_location = self.stream.get_artifact_location(artifact_hash)
            #self.bundle_locations.append(artifact_location)
            self.bundle_artifacts.append(Artifact(location=artifact_location, hash=artifact_hash))
    
    def get_detected_artifacts(self, current_run_id: int) -> List[Tuple[Any, str]]:
        if current_run_id < 0:
            raise ValueError(f"current_run_id {current_run_id} must be greater than or equal to 0 to get detected artifacts between runs.")
        
        with self.conn_manager.read_cursor() as cursor:
            if current_run_id == 0:
                query: sql = f"""
                    SELECT artifact_location, artifact_hash
                    FROM {self.stream.local_table_name}
                    WHERE timestamp <= (
                        SELECT timestamp
                        FROM run_events
                        WHERE node_name = ? AND run_id = 0 AND event_type = 'start'
                    );
                """
                cursor.execute(query, (self.node_name,))
                detected_artifacts = cursor.fetchall()
                return detected_artifacts

            else:
                query: sql = f"""
                    SELECT artifact_location, artifact_hash
                    FROM {self.stream.local_table_name}
                    WHERE timestamp > (
                        SELECT timestamp
                        FROM run_events
                        WHERE node_name = ? AND run_id = ? AND event_type = 'start'
                    )
                    AND timestamp <= (
                        SELECT timestamp
                        FROM run_events
                        WHERE node_name = ? AND run_id = ? AND event_type = 'start'
                    );
                """
                cursor.execute(query, (self.node_name, current_run_id - 1, self.node_name, current_run_id))
                detected_artifacts = cursor.fetchall()
                return detected_artifacts
    
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
                    log(f"{self.name} yielding {len(using_bundle)} artifacts from last partial bundle after restart", level="info", logger=self.logger)
                    yield using_bundle
                    #self.bundle_locations = []
                    #self.bundle_hashes = []
                    self.bundle_artifacts = []

            self.restart = 0   # reset restart mode after restart is done

            #if len(self.bundle_hashes) < self.bundle_size:
            if len(self.bundle_artifacts) < self.bundle_size:
                artifact = self.items_queue.get(block=True)

                # avoid adding duplicate artifacts to the bundle in case the same artifact is put in the queue multiple times due to restarts
                if self.is_artifact_used(artifact.hash) is False:   
                    #self.bundle_locations.append(artifact.location)
                    #self.bundle_hashes.append(artifact.hash)
                    self.bundle_artifacts.append(artifact)
            else:
                #log(f"{self.name} yielding bundle_locations: {self.bundle_locations[:self.bundle_size]}, bundle_hashes: {self.bundle_hashes[:self.bundle_size]}", level="info", logger=self.logger)
                #bundle = self.bundle_locations[:self.bundle_size]  # yield only a batch of items based on the bundle size
                bundle = self.bundle_artifacts[:self.bundle_size]  # yield only a batch of items based on the bundle size

                yield bundle

                # remove the items that were just yielded from the bundle_locations list, keep the remaining items for the next yield
                #self.bundle_locations = self.bundle_locations[self.bundle_size:]
                #self.bundle_hashes = self.bundle_hashes[self.bundle_size:]
                self.bundle_artifacts = self.bundle_artifacts[self.bundle_size:]
