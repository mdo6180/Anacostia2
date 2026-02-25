from datetime import datetime
from logging import Logger
import os
import shutil

from utils.connection import ConnectionManager

sql = str   # alias of the str type for syntax highlighting using the Python Inline Source Syntax Highlighting extension by Sam Willis in VSCode.


class FileSystemTransport:
    def __init__(self, name: str, dest_directory: str, logger: Logger = None):
        self.name = name
        self.logger = logger

        self.dest_directory = dest_directory
        if os.path.exists(dest_directory) is False:
            os.makedirs(dest_directory)

        self.global_usage_table_name = "artifact_usage_events"
    
    def initialize_db_connection(self, filename: str):
        self.conn_manager = ConnectionManager(db_path=filename, logger=self.logger)
        
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

        # register the send event in the DB, so that consumers can see that the artifact has been sent and is ready to be received 
        # (wait for acknowledgement from Stream)
        self.register_artifact_send(filepath, artifact_hash)

        if self.logger:
            self.logger.info(f"Artifact {artifact_hash} sent to {dest_path}")