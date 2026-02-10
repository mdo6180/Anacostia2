from logging import Logger
import os
import hashlib

from utils.connection import ConnectionManager



class Producer:
    def __init__(self, name: str, directory: str, hash_chunk_size: int = 1_048_576, logger: Logger = None):
        self.name = name
        self.hash_chunk_size = hash_chunk_size
        self.logger = logger
        self.run_id = 0
    
        self.directory = directory
        if os.path.exists(directory) is False:
            self.logger.info(f"Directory {directory} does not exist. Creating it.")
            os.makedirs(directory)

        self.global_usage_table_name = "artifact_usage_events"
    
    def set_run_id(self, run_id: int):
        self.run_id = run_id

    def initialize_db_connection(self, filename: str):
        self.conn_manager = ConnectionManager(db_path=filename, logger=self.logger)

    def created_artifact(self, filepath: str, artifact_hash: str) -> None:
        with self.conn_manager.write_cursor() as cursor:
            cursor.execute(
                f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} (artifact_hash, node_name, run_id, state, details)
                VALUES (?, ?, ?, ?, ?);
                """,
                (artifact_hash, self.name, self.run_id, "created", filepath)
            )
        self.logger.info(f"{self.name} created_artifact: {filepath} in run {self.run_id}")        # created_artifact DB call in future

    def hash_file(self, filepath: str) -> str:
        sha256 = hashlib.sha256()
        with open(filepath, 'rb') as f:
            while chunk := f.read(self.hash_chunk_size):
                sha256.update(chunk)
        return sha256.hexdigest()

    def write(self, filename: str, content: str):
        path = os.path.join(self.directory, filename)
        """
        if os.path.exists(path):
            # might want to consider deleting existing file instead of overwriting in future if we want to preserve file paths for artifacts in the DB, 
            # but for now we'll just overwrite and log a warning
            self.logger.warning(f"File {path} already exists. It will be overwritten.")
        """

        with open(path, "a") as file:
            file.write(content)

        artifact_hash = self.hash_file(path)
        self.created_artifact(path, artifact_hash)