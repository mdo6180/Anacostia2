import hashlib
from logging import Logger
import os
import time
from typing import Any, Generator, Tuple
from datetime import datetime

from connection import ConnectionManager



class DirectoryStream:
    def __init__(self, name: str, directory: str, poll_interval: float = 0.1, hash_chunk_size: int = 1_048_576, logger: Logger = None):
        self.logger = logger
        if os.path.exists(directory) is False:
            self.logger.info(f"Directory {directory} does not exist. Creating it.")
            os.makedirs(directory)

        self.name = name
        self.directory = directory
        self.poll_interval = poll_interval
        self.hash_chunk_size = hash_chunk_size

        self.conn_manager: ConnectionManager = None
        self.local_table_name = f"{self.name}_local"
        self.global_usage_table_name = "artifact_usage_events"

    def initialize_db_connection(self, filename: str):
        self.conn_manager = ConnectionManager(db_path=filename, logger=self.logger)
        # add logic to create necessary tables if needed
        # create a stream_artifacts table to track seen artifacts for all streams
        # add a way to get indexes from seen artifacts to help with resuming streams after restart
        # associate the file paths with file hashes in the DB for this stream
    
    def setup(self):
        with self.conn_manager.write_cursor() as cursor:
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {self.local_table_name} (
                    artifact_index INTEGER PRIMARY KEY AUTOINCREMENT,
                    artifact_path TEXT NOT NULL,
                    artifact_hash TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    hash_algorithm TEXT,
                    UNIQUE(artifact_path, artifact_hash),
                    UNIQUE(artifact_hash)
                );
                """
            )

    def register_artifact(self, filepath: str, artifact_hash: str) -> None:
        timestamp = datetime.now()

        with self.conn_manager.write_cursor() as cursor:
            cursor.execute(
                f"INSERT OR IGNORE INTO {self.local_table_name} (artifact_path, timestamp, artifact_hash, hash_algorithm) VALUES (?, ?, ?, ?);",
                (filepath, timestamp, artifact_hash, "sha256")
            )
            cursor.execute(
                f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} (artifact_hash, node_name, state, details) 
                VALUES (?, ?, ?, ?);
                """,
                (artifact_hash, self.name, "detected", filepath)
            )
    
    def artifact_registered(self, filepath: str) -> bool:
        with self.conn_manager.read_cursor() as cursor:
            cursor.execute(
                f"SELECT 1 FROM {self.local_table_name} WHERE artifact_path = ? LIMIT 1;",
                (filepath,)
            )
            return cursor.fetchone() is not None
    
    def get_artifact_path(self, artifact_hash: str) -> str:
        with self.conn_manager.read_cursor() as cursor:
            cursor.execute(
                f"SELECT artifact_path FROM {self.local_table_name} WHERE artifact_hash = ? LIMIT 1;",
                (artifact_hash,)
            )
            result = cursor.fetchone()
            if result is None:
                raise ValueError(f"Artifact with hash {artifact_hash} not found in local stream table.")
            return result[0]

    def hash_file(self, filepath: str) -> str:
        sha256 = hashlib.sha256()
        with open(filepath, 'rb') as f:
            while chunk := f.read(self.hash_chunk_size):
                sha256.update(chunk)
        return sha256.hexdigest()
    
    def __getitem__(self, index) -> Tuple[Any, str]:
        """
        Get the content and hash of the artifact at the given index in chronological order. User implemented method.
        """
        # Note: this method is useful for getting a certain artifact (such as the latest model needed for resume after a restart)
        # latest_model = stream[-1] to get the latest model artifact for example, or model = stream[model_index] to get a specific model artifact by index
        def list_files_chronological():
            with self.conn_manager.read_cursor() as cursor:
                cursor.execute(f"SELECT artifact_path, artifact_hash FROM {self.local_table_name} ORDER BY timestamp ASC;")
                return cursor.fetchall()

        artifact_entries = list_files_chronological()
        if index >= len(artifact_entries):
            raise IndexError("Index out of range for available artifacts in stream.")
        
        artifact_path, artifact_hash = artifact_entries[index]
        with open(artifact_path, "r") as file:
            content = file.read()
            return content, artifact_hash

    def __iter__(self) -> Generator[Any, Any, str]:
        """
        Yields single items: (content, file_hash). User implemented method.
        """
        while True:
            for filename in sorted(os.listdir(self.directory)):

                path = os.path.join(self.directory, filename)
                if not os.path.isfile(path):
                    continue

                if self.artifact_registered(path):  # check if we've already seen this artifact in the DB, if so skip it
                    continue

                # if the file is new, hash it, register it in the DB, and yield its content and hash
                file_hash = self.hash_file(path)

                self.register_artifact(path, file_hash)

                # Read file content, user will implement their own logic to extract the content from the artifact
                with open(path, "r") as file:
                    content = file.read()
                    yield content, file_hash

            # IMPORTANT: put this sleep here so the polling doesn't block the main thread.
            time.sleep(self.poll_interval)