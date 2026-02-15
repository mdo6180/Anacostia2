from logging import Logger
import os
import hashlib

from utils.connection import ConnectionManager

sql = str   # alias of the str type for syntax highlighting using the Python Inline Source Syntax Highlighting extension by Sam Willis in VSCode.



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

        # keep track of artifact paths created in the current run, so that we can log warnings if the same path is being overwritten in the same run
        self.paths_in_current_run = set()   
    
    def set_run_id(self, run_id: int):
        self.run_id = run_id

    def initialize_db_connection(self, filename: str):
        self.conn_manager = ConnectionManager(db_path=filename, logger=self.logger)

    def register_created_artifacts(self) -> None:
        entries = []
        for path in self.paths_in_current_run:
            artifact_hash = self.hash_artifact(path)
            entries.append((artifact_hash, self.name, self.run_id, "created", path))
            self.logger.info(f"Producer {self.name} registering created artifact {path} with hash {artifact_hash} in run {self.run_id}")

        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} 
                (artifact_hash, node_name, run_id, state, details) 
                VALUES (?, ?, ?, ?, ?);
            """
            cursor.executemany(query, entries)

    def hash_artifact(self, filepath: str) -> str:
        sha256 = hashlib.sha256()
        with open(filepath, 'rb') as f:
            while chunk := f.read(self.hash_chunk_size):
                sha256.update(chunk)
        return sha256.hexdigest()

    def create_artifact(self, filename: str, content: str):
        # check if the path already exists in the current run, if not, add it to the set. 
        # at the end of the run, hash and create DB entry for all paths in the set, then clear the set for the next run. 
        # This way we can avoid hashing the same file multiple times in the same run and also preserve file paths for artifacts in the DB.

        path = os.path.join(self.directory, filename)
        if path not in self.paths_in_current_run:
            self.paths_in_current_run.add(path)

        with open(path, "a") as file:
            file.write(content)
