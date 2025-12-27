import threading
from queue import Queue
from typing import Dict
from abc import ABC, abstractmethod
from logging import Logger
import os
import time
import traceback
from threading import Thread
from datetime import datetime
import hashlib

from utils.connection import ConnectionManager
from utils.signal import Signal



class BaseWatcherNode(threading.Thread, ABC):
    def __init__(self, name: str, path: str, hash_chunk_size: int = 1_048_576, logger: Logger = None):
        self.path = path
        if os.path.exists(self.path) is False:
            os.makedirs(self.path)

        self.successor_queues: Dict[str, Queue] = {}
        self.exit_event = threading.Event()
        self.resource_event = threading.Event()
        self.logger = logger
        self.conn_manager: ConnectionManager = None
        self.hash_chunk_size = hash_chunk_size
        self.artifact_table_name = f"{name}_{abs(hash(f'{name}_{path}'))}_artifacts"

        self.global_usage_table_name = "artifact_usage_events"

        self.run_id = 0

        super().__init__(name=name)
    
    def __hash__(self):
        return abs(hash(f"{self.name}_{self.path}"))
    
    def log(self, message: str, level="DEBUG") -> None:
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

    def set_successor_queue(self, successor_name: str, queue: Queue):
        self.successor_queues[successor_name] = queue
    
    def initialize_db_connection(self, filename: str):
        self.conn_manager = ConnectionManager(filename)
    
    def setup(self):
        with self.conn_manager.write_cursor() as cursor:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.artifact_table_name} (
                    artifact_path TEXT UNIQUE NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    artifact_hash TEXT PRIMARY KEY,
                    hash_algorithm TEXT
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
        self.observer_thread = Thread(name=f"{self.name}_observer", target=_monitor_thread_func, daemon=True)
        self.observer_thread.start()
    
    def register_artifact(self, filepath: str) -> None:
        timestamp = datetime.now()

        with self.conn_manager.write_cursor() as cursor:
            cursor.execute(
                f"INSERT INTO {self.artifact_table_name} (artifact_path, timestamp, artifact_hash, hash_algorithm) VALUES (?, ?, ?, ?);",
                (filepath, timestamp, self.hash_file(filepath), "sha256")
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
                WHERE artifact_hash NOT IN (SELECT DISTINCT artifact_hash FROM {self.global_usage_table_name} WHERE node_id = ?);
                """,
                (hash(self),)
            )
            return cursor.fetchall()
    
    def mark_artifact_used(self, filepath: str, artifact_hash: str) -> None:
        with self.conn_manager.write_cursor() as cursor:
            cursor.execute(
                f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} (artifact_path, artifact_hash, node_id, run_id, usage_type)
                VALUES (?, ?, ?, ?, ?);
                """,
                (filepath, artifact_hash, hash(self), self.run_id, "read")
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
        for successor_name, queue in self.successor_queues.items():
            signal = Signal(source_node_name=self.name, source_run_id=self.run_id, timestamp=datetime.now())
            queue.put(signal)
            self.log(f"{self.name} signalled {successor_name}", level="INFO")
    
    @abstractmethod
    def execute(self):
        """
        Override to specify what the node does when triggered.
        This method is called when the resource_event is set.
        """
        pass
    
    def run(self):
        self.start_monitoring()     # Start monitoring the resource in a separate thread

        while not self.exit_event.is_set():

            if self.exit_event.is_set(): return
            self.resource_event.wait()      # Wait until the resource event is set

            if self.exit_event.is_set(): return
            self.conn_manager.start_run(self.name, self.run_id)
            self.execute()
            self.conn_manager.end_run(self.name, self.run_id)

            if self.exit_event.is_set(): return
            self.signal_successors()

            self.run_id += 1

            if self.exit_event.is_set(): return
            self.resource_event.clear()     # Reset the event for the next cycle