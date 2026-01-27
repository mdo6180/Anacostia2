import threading
from abc import ABC, abstractmethod
from logging import Logger
import os
import time
import traceback
from datetime import datetime
import hashlib
from typing import List, Callable
from io import TextIOWrapper
import os
import tempfile
import glob

from utils.connection import ConnectionManager



class OutputFileManager:
    def __init__(self, node: 'BaseWatcherNode', filename, mode):
        self.node = node
        self.filename = filename
        self.final_path = os.path.join(node.output_path, filename)
        self.mode = mode

        self.file: TextIOWrapper = None
        self.tmp_file: TextIOWrapper = None
        self.tmp_path: str = None

    def __enter__(self):
        def cleanup_stale_tmp_files(output_dir: str, filename: str):
            pattern = os.path.join(output_dir, f".tmp_{filename}.*")
            for path in glob.glob(pattern):
                try:
                    os.unlink(path)
                except FileNotFoundError:
                    pass
                except PermissionError:
                    # If something is still writing it, skip
                    pass

        cleanup_stale_tmp_files(self.node.output_path, self.filename)

        # Create temp file in same directory for atomic replace
        tmp_fd, self.tmp_path = tempfile.mkstemp(
            dir=self.node.output_path,
            prefix=f".tmp_{self.filename}.",
            text="b" not in self.mode
        )

        # Wrap fd in a Python file object
        self.tmp_file = os.fdopen(tmp_fd, self.mode)
        self.file = self.tmp_file
        return self.file

    def __exit__(self, exc_type, exc_value, exc_traceback):
        try:
            if self.file:
                # Ensure Python buffers flushed
                self.file.flush()
                # Ensure OS buffers flushed
                os.fsync(self.file.fileno())
                self.file.close()

            # Only commit if we're not shutting down and no exception
            if exc_type is None and not self.node.exit_event.is_set():
                # Atomic replace: temp → final
                os.replace(self.tmp_path, self.final_path)

                # Mark committed against final path
                artifact_hash = self.node.hash_file(self.final_path)
                self.node.mark_artifact_committed(filepath=self.final_path, artifact_hash=artifact_hash)
            else:
                # On error or shutdown, do NOT replace final file
                if self.tmp_path and os.path.exists(self.tmp_path):
                    os.unlink(self.tmp_path)

        finally:
            self.file = None
            self.tmp_file = None
            self.tmp_path = None



class InputFileManager:
    def __init__(self, node: 'BaseWatcherNode', filename: str, artifact_hash: str):
        self.node = node
        self.filename = filename
        self.filepath = os.path.join(node.input_path, filename)
        self.artifact_hash = artifact_hash
        self.mode = 'r'
        self.file: TextIOWrapper = None
        
    def __enter__(self):
        self.node.mark_artifact_using(self.filepath, self.artifact_hash)
        self.file = open(self.filepath, self.mode)
        return self.file
    
    def __exit__(self, exc_type, exc_value, exc_traceback):
        if exc_type is KeyboardInterrupt:
            pass
        else:
            self.node.mark_artifact_used(self.filepath, self.artifact_hash)

        self.file.close()



class BaseWatcherNode(threading.Thread, ABC):
    def __init__(self, name: str, input_path: str, output_path: str, hash_chunk_size: int = 1_048_576, logger: Logger = None):
        self.input_path = input_path
        if os.path.exists(self.input_path) is False:
            os.makedirs(self.input_path)
        
        self.output_path = output_path
        if os.path.exists(self.output_path) is False:
            os.makedirs(self.output_path)

        self.successors = []
        self.exit_event = threading.Event()
        self.resource_event = threading.Event()
        self.logger = logger
        self.conn_manager: ConnectionManager = None
        self.hash_chunk_size = hash_chunk_size
        
        self.node_id = f"{name}|{self.input_path}|{self.output_path}"
        self.node_id = hashlib.sha256(self.node_id.encode("utf-8")).hexdigest()
        self.artifact_table_name = f"{name}_{self.node_id}_artifacts"

        self.global_usage_table_name = "artifact_usage_events"
        self.filtered_artifacts_list: List[tuple[str, str]] = []    # list of (artifact_path, artifact_hash)
        self.filtering_func: Callable[[str], bool] = None

        self.run_id = 0

        self.resuming = False

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
                for root, dirnames, filenames in os.walk(self.input_path):
                    for filename in filenames:
                        filepath = os.path.join(root, filename)
                        
                        try:
                            if self.artifact_exists(filepath) is False:
                                self.log(f"{self.name} detected file {filepath}", level="INFO")
                                self.register_artifact(filepath)
                        
                        except Exception as e:
                            self.log(f"Unexpected error in monitoring logic for '{self.name}': {traceback.format_exc()}", level="ERROR")
                
                # During resuming, do not mark new artifacts as primed
                if self.resuming is False:
                    previous_filtered_artifacts_hashes = set(artifact_hash for artifact_path, artifact_hash in self.get_filtered_artifacts())
                    
                    current_detected_artifacts = self.__get_detected_artifacts()
                    current_detected_artifacts_hashes = set(artifact_hash for artifact_path, artifact_hash in current_detected_artifacts)
                    
                    if previous_filtered_artifacts_hashes != current_detected_artifacts_hashes:
                        self.filtered_artifacts_list = current_detected_artifacts

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
                f"INSERT OR IGNORE INTO {self.artifact_table_name} (artifact_path, timestamp, artifact_hash, hash_algorithm) VALUES (?, ?, ?, ?);",
                (filepath, timestamp, self.hash_file(filepath), "sha256")
            )
            cursor.execute(
                f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} (artifact_path, artifact_hash, node_id, node_name, state, edge_type) 
                VALUES (?, ?, ?, ?, ?, ?);
                """,
                (filepath, self.hash_file(filepath), self.node_id, self.name, "detected", "input")
            )
    
    def artifact_exists(self, filepath: str) -> bool:
        with self.conn_manager.read_cursor() as cursor:
            cursor.execute(
                f"SELECT 1 FROM {self.artifact_table_name} WHERE artifact_path = ? LIMIT 1;",
                (filepath,)
            )
            return cursor.fetchone() is not None
    
    def __get_detected_artifacts(self) -> list:
        with self.conn_manager.read_cursor() as cursor:
            cursor.execute(
                f"""
                SELECT artifact_path, artifact_hash FROM {self.global_usage_table_name}
                WHERE node_id = ? AND state = 'detected' AND artifact_hash NOT IN (
                SELECT DISTINCT artifact_hash FROM {self.global_usage_table_name} WHERE node_id = ? AND state in ('primed', 'using', 'used', 'ignored')
                );
                """,
                (self.node_id, self.node_id)
            )

            if self.filtering_func is not None:
                artifacts = cursor.fetchall()
                filtered_artifacts = []
                for artifact_tuple in artifacts:
                    artifact_path, artifact_hash = artifact_tuple

                    if self.filtering_func(artifact_path) is True:
                        filtered_artifacts.append(artifact_tuple)
                        self.mark_artifact_primed(artifact_path, artifact_hash)
                    else:
                        self.mark_artifact_ignored(artifact_path, artifact_hash)

                return filtered_artifacts

            return cursor.fetchall()
    
    def get_filtered_artifacts(self) -> list:
        return self.filtered_artifacts_list
    
    def set_filtering_function(self, func: Callable[[str], bool]) -> None:
        """ Set the filtering function to filter detected artifacts. """
        self.filtering_func = func
    
    def mark_artifact_primed(self, filepath: str, artifact_hash: str) -> None:
        with self.conn_manager.write_cursor() as cursor:
            cursor.execute(
                f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} (artifact_path, artifact_hash, node_id, node_name, run_id, state, edge_type)
                VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (filepath, artifact_hash, self.node_id, self.name, self.run_id, "primed", "input")
            )
    
    def mark_artifact_using(self, filepath: str, artifact_hash: str) -> None:
        with self.conn_manager.write_cursor() as cursor:
            cursor.execute(
                f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} (artifact_path, artifact_hash, node_id, node_name, run_id, state, edge_type)
                VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (filepath, artifact_hash, self.node_id, self.name, self.run_id, "using", "input")
            )
    
    def mark_artifact_used(self, filepath: str, artifact_hash: str) -> None:
        with self.conn_manager.write_cursor() as cursor:
            cursor.execute(
                f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} (artifact_path, artifact_hash, node_id, node_name, run_id, state, edge_type)
                VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (filepath, artifact_hash, self.node_id, self.name, self.run_id, "used", "input")
            )
    
    def mark_artifact_ignored(self, filepath: str, artifact_hash: str) -> None:
        with self.conn_manager.write_cursor() as cursor:
            cursor.execute(
                f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} (artifact_path, artifact_hash, node_id, node_name, run_id, state, edge_type)
                VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (filepath, artifact_hash, self.node_id, self.name, self.run_id, "ignored", "input")
            )
    
    def mark_artifact_created(self, filepath: str, artifact_hash: str) -> None:
        with self.conn_manager.write_cursor() as cursor:
            cursor.execute(
                f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} (artifact_path, artifact_hash, node_id, node_name, run_id, state, edge_type)
                VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (filepath, artifact_hash, self.node_id, self.name, self.run_id, "created", "output")
            )
    
    def mark_artifact_accessed(self, filepath: str, artifact_hash: str) -> None:
        with self.conn_manager.write_cursor() as cursor:
            cursor.execute(
                f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} (artifact_path, artifact_hash, node_id, node_name, run_id, state, edge_type)
                VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (filepath, artifact_hash, self.node_id, self.name, self.run_id, "accessed", "output")
            )
    
    def mark_artifact_committed(self, filepath: str, artifact_hash: str) -> None:
        with self.conn_manager.write_cursor() as cursor:
            cursor.execute(
                f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} (artifact_path, artifact_hash, node_id, node_name, run_id, state, edge_type)
                VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (filepath, artifact_hash, self.node_id, self.name, self.run_id, "committed", "output")
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
    
    def resume(self):
        pass

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

            self.log(f"{self.name} restarting run {self.run_id}", level="INFO")
            self.conn_manager.resume_run(self.name, self.run_id)

            self.resuming = True    # tell the monitoring thread we are resuming and should not mark new artifacts as primed

            # load filtered artifacts that were primed before the restart
            # this ensures that we re-process any artifacts that were primed but not yet processed
            with self.conn_manager.read_cursor() as cursor:
                cursor.execute(
                    f"""
                    SELECT artifact_path, artifact_hash FROM {self.global_usage_table_name}
                    WHERE node_id = ? AND run_id = ? AND state = 'primed';
                    """,
                    (self.node_id, self.run_id)
                )
                self.filtered_artifacts_list = cursor.fetchall()

            # run the resume logic to set up any necessary state (e.g., load model from checkpoint, etc.)
            self.resume()

            # there is a problem on the restart where it wasn't done processing all artifacts that were detected before the restart
            # but then on the restart it skipped ahead to the next artifact instead of re-processing the previous one
            # so we need to re-execute to process all artifacts whose state is either 'primed' or 'using'.
            # start with 'using' first to ensure that any artifacts that were in the process of being used are completed, then move to 'primed' artifacts.
            try:
                self.execute()
            except Exception as e:
                self.log(f"Error during restart execution in node '{self.name}': {traceback.format_exc()}", level="ERROR")

            self.log(f"{self.name} finished run {self.run_id}", level="INFO")
            self.conn_manager.end_run(self.name, self.run_id)

            self.resuming = False     # tell the monitoring thread we are done resuming and can mark new artifacts as primed

            self.signal_successors()

            self.run_id += 1
            self.resource_event.clear()     # Reset the event for the next cycle

        while not self.exit_event.is_set():

            if self.exit_event.is_set(): return
            self.resource_event.wait()      # Wait until the resource event is set

            if self.exit_event.is_set(): return
            self.conn_manager.start_run(self.name, self.run_id)
            self.log(f"{self.name} starting new run {self.run_id}", level="INFO")

            try:
                self.execute()
            except Exception as e:
                self.log(f"Error during restart execution in node '{self.name}': {traceback.format_exc()}", level="ERROR")
            
            self.log(f"{self.name} finished run {self.run_id}", level="INFO")
            self.conn_manager.end_run(self.name, self.run_id)

            # we don't wait for the exit event here to allow successors to be signalled even if we are terminating
            # if the node exits, we want it to exit before the end_run is called, otherwise, we want to log the run into the run graph
            self.signal_successors()

            self.run_id += 1

            if self.exit_event.is_set(): return
            self.resource_event.clear()     # Reset the event for the next cycle