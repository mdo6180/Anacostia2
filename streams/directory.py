import hashlib
from logging import Logger
import os
import time
from typing import Any, Generator
from pathlib import Path

from utils.connection import ConnectionManager

sql = str   # alias of the str type for syntax highlighting using the Python Inline Source Syntax Highlighting extension by Sam Willis in VSCode.



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
        """
        Create the local table for this stream to track seen artifacts and their hashes.
        User implemented method (call super().setup() if overriding).
        To add additonal columns to the local table, user can execute an ALTER TABLE statement in their overridden setup() method.
        To add additional tables, user can execute CREATE TABLE statements in their overridden setup() method.
        Make sure table names are unique to avoid conflicts with other streams and producers. 
        We recommend using the convention of prefixing table names with the stream or producer name, 
        e.g. {stream_name}_artifacts for a stream's local table to track artifacts.
        """
        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
                CREATE TABLE IF NOT EXISTS {self.local_table_name} (
                    artifact_index INTEGER PRIMARY KEY AUTOINCREMENT,
                    artifact_path TEXT NOT NULL,
                    artifact_hash TEXT NOT NULL,
                    node_name TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    hash_algorithm TEXT,
                    UNIQUE(artifact_path, artifact_hash),
                    UNIQUE(artifact_hash)
                );
            """
            cursor.execute(query)

    def register_artifact(self, filepath: str, artifact_hash: str) -> None:
        """
        Register the artifact in the local stream table and the global usage table in the DB.
        User implemented method (call super().register_artifact() if overriding).
        If overriding, remember to add whatever additional data to fill out the additional columns in the local table 
        if you added additional columns in the setup() method.
        """
        self.register_artifact_local(artifact_hash, filepath)
        self.register_artifact_global(artifact_hash)
    
    def register_artifact_local(self, artifact_hash: str, filepath: str = None) -> None:
        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
                INSERT OR IGNORE INTO {self.local_table_name} 
                (artifact_path, artifact_hash, node_name, hash_algorithm) 
                VALUES (?, ?, ?, ?);
            """
            cursor.execute(query, (filepath, artifact_hash, self.name, "sha256"))

    def register_artifact_global(self, artifact_hash: str) -> None:
        artifact_path = self.get_artifact_path(artifact_hash)
        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} 
                (artifact_hash, node_name, state, details) 
                VALUES (?, ?, ?, ?);
            """
            cursor.execute(query, (artifact_hash, self.name, "detected", artifact_path))
            # self.logger.info(f"Registered artifact {filepath} with hash {artifact_hash} in stream {self.name} at {timestamp}")
    
    def is_artifact_registered(self, filepath: str) -> bool:
        with self.conn_manager.read_cursor() as cursor:
            query: sql = f"""
                SELECT 1 FROM {self.local_table_name} WHERE artifact_path = ? LIMIT 1;
            """
            cursor.execute(query, (filepath,))
            return cursor.fetchone() is not None
    
    def get_artifact_path(self, artifact_hash: str) -> str:
        with self.conn_manager.read_cursor() as cursor:
            query: sql = f"""
                SELECT artifact_path FROM {self.local_table_name} WHERE artifact_hash = ? LIMIT 1;
            """
            cursor.execute(query, (artifact_hash,))
            result = cursor.fetchone()
            if result is None:
                raise ValueError(f"Artifact with hash {artifact_hash} not found in local stream table.")
            return result[0]

    def hash_artifact(self, filepath: str) -> str:
        """
        Hash the artifact using the specified hash algorithm and return the hash value. User implemented method.
        """
        sha256 = hashlib.sha256()
        with open(filepath, 'rb') as f:
            while chunk := f.read(self.hash_chunk_size):
                sha256.update(chunk)
        return sha256.hexdigest()
    
    def hash_directory(self, directory: str) -> str:
        """
        Compute a deterministic SHA256 hash of a directory and all its contents.

        Hash includes:
        - relative file paths
        - file contents

        Files are processed in sorted order to ensure determinism.
        """

        root = Path(directory).resolve()

        if not root.is_dir():
            raise ValueError(f"{directory} is not a directory")

        hasher = hashlib.sha256()

        # Walk files in deterministic order
        for path in sorted(root.rglob("*")):
            if path.is_file():
                rel_path = path.relative_to(root)

                # Hash relative path first (prevents rename collisions)
                hasher.update(str(rel_path).encode("utf-8"))
                hasher.update(b"\0")

                # Hash file contents
                with open(path, "rb") as f:
                    while chunk := f.read(self.hash_chunk_size):
                        hasher.update(chunk)

                hasher.update(b"\0")

        return hasher.hexdigest()

    def load_artifact(self, artifact_hash: str) -> str:
        """
        Load and return the content of the artifact given its hash. User implemented method.
        """
        artifact_path = self.get_artifact_path(artifact_hash)
        with open(artifact_path, "r") as file:
            content = file.read()
            return content
    
    def __iter__(self) -> Generator[Any, Any, str]:
        """
        Poll the resource for new artifacts, register the artifacts into the DB, and yield their content and hashes.
        Yields single items: (content, file_hash). User implemented method.
        """
        directory = Path(self.directory)

        while True:
            # sort files by last modification time
            for path in sorted(directory.iterdir(), key=lambda p: p.stat().st_mtime):

                path_str = str(path)

                # skip artifacts already registered
                if self.is_artifact_registered(path_str):
                    continue

                # hash and register artifact
                if path.is_file():
                    file_hash = self.hash_artifact(path_str)
                    self.register_artifact(path_str, file_hash)
                
                elif path.is_dir():
                    file_hash = self.hash_directory(path_str)
                    self.register_artifact(path_str, file_hash)
                
                else:
                    raise ValueError(f"Unsupported artifact type for path {path_str}")

                # user-defined content loader
                content = self.load_artifact(file_hash)

                yield content, file_hash

            # IMPORTANT: prevent polling from blocking main thread
            time.sleep(self.poll_interval)