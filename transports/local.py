from logging import Logger
import os
import shutil

from utils.connection import ConnectionManager

sql = str   # alias of the str type for syntax highlighting using the Python Inline Source Syntax Highlighting extension by Sam Willis in VSCode.


class FileSystemTransport:
    def __init__(self, name: str, dest_directory: str, dest_stream_name: str, logger: Logger = None):
        self.name = name
        self.logger = logger
        self.dest_stream_name = dest_stream_name
        self.local_table_name = f"{self.name}_local"
        self.global_usage_table_name = "artifact_usage_events"

        self.dest_directory = dest_directory
        if os.path.exists(dest_directory) is False:
            os.makedirs(dest_directory)
    
    def initialize_db_connection(self, filename: str):
        self.conn_manager = ConnectionManager(db_path=filename, logger=self.logger)

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
                    artifact_src_path TEXT NOT NULL,
                    artifact_dest_path TEXT NOT NULL,
                    artifact_hash TEXT NOT NULL,
                    node_name TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    hash_algorithm TEXT,
                    UNIQUE(artifact_src_path, artifact_hash),
                    UNIQUE(artifact_hash)
                );
            """
            cursor.execute(query)

    def register_artifact_send(self, filepath: str, artifact_hash: str) -> None:
        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} 
                (artifact_hash, node_name, state, details) 
                VALUES (?, ?, ?, ?);
            """
            cursor.execute(query, (artifact_hash, self.name, "sent", filepath))

    def send(self, filepath: str, artifact_hash: str) -> None:
        filename = os.path.basename(filepath)
        dest_path = os.path.join(self.dest_directory, filename)
        
        # the protocol to actually send the artifact
        shutil.copy2(filepath, dest_path)

        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
                INSERT OR IGNORE INTO {self.local_table_name} 
                (artifact_src_path, artifact_dest_path, artifact_hash, node_name, hash_algorithm) 
                VALUES (?, ?, ?, ?, ?);
            """
            cursor.execute(query, (filepath, dest_path, artifact_hash, self.name, "sha256"))
        
        self.register_artifact_send(filepath=dest_path, artifact_hash=artifact_hash)