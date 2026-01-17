import threading
from abc import ABC, abstractmethod
from logging import Logger
import os
import time
import traceback
from datetime import datetime
import hashlib
from typing import List

from utils.connection import ConnectionManager



class BaseWatcherNode(threading.Thread, ABC):
    def __init__(self, name: str, path: str, hash_chunk_size: int = 1_048_576, logger: Logger = None):
        self.path = path
        if os.path.exists(self.path) is False:
            os.makedirs(self.path)

        self.successors = []
        self.exit_event = threading.Event()
        self.resource_event = threading.Event()
        self.logger = logger
        self.conn_manager: ConnectionManager = None
        self.hash_chunk_size = hash_chunk_size
        
        self.node_id = f"{name}|{self.path}"
        self.node_id = hashlib.sha256(self.node_id.encode("utf-8")).hexdigest()
        self.artifact_table_name = f"{name}_{self.node_id}_artifacts"

        self.global_usage_table_name = "artifact_usage_events"
        self.filtered_artifacts_list: List[tuple[str, str]] = []

        self.run_id = 0

        super().__init__(name=name)
    
    def log(self, message: str, level="DEBUG", color: str | None = None) -> None:
        ANSI_COLORS = {
            "black": "\033[30m",
            "red": "\033[31m",
            "green": "\033[32m",
            "yellow": "\033[33m",
            "blue": "\033[34m",
            "magenta": "\033[35m",
            "cyan": "\033[36m",
            "white": "\033[37m",
            "reset": "\033[0m",
        }

        if color is not None:
            if color not in ANSI_COLORS:
                raise ValueError(f"Invalid color: {color}")
            message = f"{ANSI_COLORS[color]}{message}{ANSI_COLORS['reset']}"

        if self.logger is not None:
            if level == "DEBUG":
                self.logger.debug(message)
            elif level == "INFO":
                self.logger.info(message)
            elif level == "WARNING":
                self.logger.warning(message)
            elif level == "ERROR":
                self.logger.error(message)
            elif level == "CRITICAL":
                self.logger.critical(message)
            else:
                raise ValueError(f"Invalid log level: {level}")
        else:
            print(message)

    def initialize_db_connection(self, filename: str):
        self.conn_manager = ConnectionManager(db_path=filename, logger=self.logger)
    
    def setup(self):
        with self.conn_manager.write_cursor() as cursor:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.artifact_table_name} (
                    artifact_path TEXT UNIQUE NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    artifact_hash TEXT PRIMARY KEY,
                    hash_algorithm TEXT,
                    source TEXT,
                    UNIQUE(artifact_path, artifact_hash)
                );
                """
            )

    def exit(self):
        self.conn_manager.close()
        self.stop_monitoring()
        self.resource_event.set()
    
    def start_monitoring(self) -> None:
        """
        Override to specify how the resource is monitored. 
        Typically, this method will be used to start an observer that runs in a child thread spawned by the thread running the node.
        """

        def _monitor_thread_func():
            self.log(f"Starting observer thread for node '{self.name}'", level="INFO")
            while self.exit_event.is_set() is False:
                for root, dirnames, filenames in os.walk(self.path):
                    for filename in filenames:
                        filepath = os.path.join(root, filename)
                        
                        try:
                            if self.artifact_exists(filepath) is False:
                                self.log(f"{self.name} detected file {filepath}", level="INFO")
                                self.register_artifact(filepath)
                        
                        except Exception as e:
                            self.log(f"Unexpected error in monitoring logic for '{self.name}': {traceback.format_exc()}", level="ERROR")
                
                filtered_artifacts_hashes = set(artifact_hash for artifact_path, artifact_hash in self.get_filtered_artifacts())
                all_detected_artifacts_hashes = set(artifact_hash for artifact_path, artifact_hash in self._get_detected_artifacts())
                if filtered_artifacts_hashes != all_detected_artifacts_hashes:
                    self.filtered_artifacts_list = self._get_detected_artifacts()

                if self.exit_event.is_set() is True: 
                    self.log(f"Observer thread for node '{self.name}' exiting", level="INFO")
                    return
                try:
                    self.resource_trigger()
                
                except Exception as e:
                    self.log(f"Error checking resource in node '{self.name}': {traceback.format_exc()}", level="ERROR")

                # sleep for a while before checking again
                time.sleep(0.1)

            self.log(f"Observer thread for node '{self.name}' exited", level="INFO")

        # since we are using asyncio.run, we need to create a new thread to run the event loop 
        # because we can't run an event loop in the same thread as the FilesystemStoreNode
        self.observer_thread = threading.Thread(name=f"{self.name}_observer", target=_monitor_thread_func, daemon=True)
        self.observer_thread.start()
    
    def register_artifact(self, filepath: str) -> None:
        timestamp = datetime.now()

        with self.conn_manager.write_cursor() as cursor:
            cursor.execute(
                f"INSERT OR IGNORE INTO {self.artifact_table_name} (artifact_path, timestamp, artifact_hash, hash_algorithm, source) VALUES (?, ?, ?, ?, ?);",
                (filepath, timestamp, self.hash_file(filepath), "sha256", "detected")
            )
            cursor.execute(
                f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} (artifact_path, artifact_hash, node_id, node_name, state, source) 
                VALUES (?, ?, ?, ?, ?, ?);
                """,
                (filepath, self.hash_file(filepath), self.node_id, self.name, "detected", "detected")
            )
    
    def artifact_exists(self, filepath: str) -> bool:
        with self.conn_manager.read_cursor() as cursor:
            cursor.execute(
                f"SELECT 1 FROM {self.artifact_table_name} WHERE artifact_path = ? LIMIT 1;",
                (filepath,)
            )
            return cursor.fetchone() is not None
    
    def get_unused_artifacts(self) -> list:
        with self.conn_manager.read_cursor() as cursor:
            cursor.execute(
                f"""
                SELECT artifact_path, artifact_hash FROM {self.artifact_table_name}
                WHERE artifact_hash NOT IN (
                SELECT DISTINCT artifact_hash FROM {self.global_usage_table_name} WHERE node_id = ? AND state = 'used'
                );
                """,
                (self.node_id,)
            )
            return cursor.fetchall()
    
    def _get_detected_artifacts(self) -> list:
        with self.conn_manager.read_cursor() as cursor:
            cursor.execute(
                f"""
                SELECT artifact_path, artifact_hash FROM {self.global_usage_table_name}
                WHERE node_id = ? AND state = 'detected' AND artifact_hash NOT IN (
                SELECT DISTINCT artifact_hash FROM {self.global_usage_table_name} WHERE node_id = ? AND state in ('using', 'used', 'ignored')
                );
                """,
                (self.node_id, self.node_id)
            )
            return cursor.fetchall()
    
    def get_filtered_artifacts(self) -> list:
        return self.filtered_artifacts_list
    
    def mark_artifact_using(self, filepath: str, artifact_hash: str) -> None:
        with self.conn_manager.write_cursor() as cursor:
            cursor.execute(
                f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} (artifact_path, artifact_hash, node_id, node_name, run_id, state, source)
                VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (filepath, artifact_hash, self.node_id, self.name, self.run_id, "using", "detected")
            )
    
    def mark_artifact_used(self, filepath: str, artifact_hash: str) -> None:
        with self.conn_manager.write_cursor() as cursor:
            cursor.execute(
                f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} (artifact_path, artifact_hash, node_id, node_name, run_id, state, source)
                VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (filepath, artifact_hash, self.node_id, self.name, self.run_id, "used", "detected")
            )

    def hash_file(self, filepath: str) -> str:
        sha256 = hashlib.sha256()
        with open(filepath, 'rb') as f:
            while chunk := f.read(self.hash_chunk_size):
                sha256.update(chunk)
        return sha256.hexdigest()
    
    def stop_monitoring(self) -> None:
        """
        Override to specify how the resource is monitored. 
        Typically, this method will be used to start an observer that runs in a child thread spawned by the thread running the node.
        """
        self.log(f"Stopping observer thread for node '{self.name}'", level="INFO")
        self.observer_thread.join()
        self.log(f"Observer stopped for node '{self.name}'", level="INFO")

    @abstractmethod
    def resource_trigger(self) -> None:
        """
        Override to specify how the resource triggers the node.
        This method is called periodically by the monitoring thread.
        When the resource condition is met, this method should set the resource_event.
        """
        pass

    def trigger(self, message: str = None) -> None:
        if self.resource_event.is_set() is False:
            self.resource_event.set()
            self.log(f"{self.name} triggered with message: {message}", level="INFO")
    
    def signal_successors(self):
        for successor in self.successors:
            self.conn_manager.signal_successor(
                source_node_name=self.name,
                source_run_id=self.run_id,
                target_node_name=successor
            )
            self.log(f"{self.name} signalled {successor}", level="INFO")

    @abstractmethod
    def execute(self):
        """
        Override to specify what the node does when triggered.
        This method is called when the resource_event is set.
        """
        pass
    
    def run(self):
        self.start_monitoring()     # Start monitoring the resource in a separate thread

        latest_run_id = self.conn_manager.get_latest_run_id(node_name=self.name)

        if latest_run_id == -1:
            self.run_id = 0

        elif self.conn_manager.run_ended(self.name, latest_run_id) is True:
            # upon restart, if the latest run has ended, start a new run
            self.run_id = latest_run_id + 1
        else:
            # upon restart, if the latest run has not ended, resume from that run
            self.run_id = latest_run_id

            #self.resource_event.wait()      # Wait until the resource event is set

            self.log(f"{self.name} restarting run {self.run_id}", level="INFO")
            self.conn_manager.resume_run(self.name, self.run_id)

            # there is a problem on the restart where it wasn't done processing all artifacts that were detected before the restart
            # but then on the restart it skipped ahead to the next artifact instead of re-processing the previous one
            # so we need to re-execute to process all unused artifacts
            self.execute()

            self.log(f"{self.name} finished run {self.run_id}", level="INFO")
            self.conn_manager.end_run(self.name, self.run_id)

            self.signal_successors()

            self.run_id += 1
            self.resource_event.clear()     # Reset the event for the next cycle

        while not self.exit_event.is_set():

            if self.exit_event.is_set(): return
            self.resource_event.wait()      # Wait until the resource event is set

            if self.exit_event.is_set(): return
            self.conn_manager.start_run(self.name, self.run_id)
            self.log(f"{self.name} starting new run {self.run_id}", level="INFO")

            self.execute()
            
            self.log(f"{self.name} finished run {self.run_id}", level="INFO")
            self.conn_manager.end_run(self.name, self.run_id)

            # we don't wait for the exit event here to allow successors to be signalled even if we are terminating
            # if the node exits, we want it to exit before the end_run is called, otherwise, we want to log the run into the run graph
            self.signal_successors()

            self.run_id += 1

            if self.exit_event.is_set(): return
            self.resource_event.clear()     # Reset the event for the next cycle