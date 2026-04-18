from logging import Logger
import os
import hashlib
from typing import List, Tuple
from pathlib import Path
import shutil

from utils.connection import ConnectionManager

sql = str   # alias of the str type for syntax highlighting using the Python Inline Source Syntax Highlighting extension by Sam Willis in VSCode.



class Producer:
    def __init__(self, name: str, directory: str, hash_chunk_size: int = 1_048_576, logger: Logger = None):
        self.name = name
        self.hash_chunk_size = hash_chunk_size
        self.logger = logger
        self.run_id = 0

        self.directory = Path(directory)
        if not self.directory.exists():
            self.logger.info(f"Directory {self.directory} does not exist. Creating it.")
            self.directory.mkdir(parents=True, exist_ok=True)

        self.global_usage_table_name = "artifact_usage_events"
        self.local_table_name = f"{self.name}_local"

        # keep track of artifact paths created in the current run, so that we can log warnings if the same path is being overwritten in the same run
        self.paths_in_current_run = set()   
    
    def setup(self):
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

    def set_db_folder(self, db_folder: str):
        self.db_folder = db_folder
        
    def initialize_staging_directory(self):
        self.staging_directory = os.path.join(self.db_folder, self.name)
        if os.path.exists(self.staging_directory) is False:
            self.logger.info(f"Temporary directory {self.staging_directory} does not exist. Creating it.")
            os.makedirs(self.staging_directory)

    def get_staging_directory(self) -> str:
        return Path(self.staging_directory)
    
    def get_final_directory(self) -> str:
        return self.directory
    
    def get_artifact_hash(self, artifact_path: str) -> str:
        with self.conn_manager.read_cursor() as cursor:
            query: sql = f"""
                SELECT artifact_hash FROM {self.local_table_name} 
                WHERE artifact_path = ?;
            """
            cursor.execute(query, (artifact_path,))
            result = cursor.fetchone()
            if result is not None:
                return result[0]
            else:
                raise ValueError(f"Artifact path {artifact_path} not found in local table {self.local_table_name}.")
    
    def set_run_id(self, run_id: int):
        self.run_id = run_id

    def initialize_db_connection(self, filename: str):
        self.conn_manager = ConnectionManager(db_path=filename, logger=self.logger)
    
    def clear_staging_directory(self):
        # clear any temp files in the staging directory from previous runs, so that we don't have any leftover temp files when we start a new run
        for filename in os.listdir(self.staging_directory):
            path = os.path.join(self.staging_directory, filename)
            if os.path.isfile(path):
                self.logger.warning(f"Producer {self.name} found leftover file {path} in staging directory from previous run. Removing it.")
                os.remove(path)
            elif os.path.isdir(path):
                self.logger.warning(f"Producer {self.name} found leftover directory {path} in staging directory from previous run. Removing it.")
                shutil.rmtree(path)
    
    def restart_producer(self):
        self.clear_staging_directory()
        
    def commit_artifact(self, artifact_staging_path: Path, artifact_final_path: Path) -> Tuple[Path, str]:
        """
        Commit an artifact by moving it from the staging directory to the final directory,
        hashing it, and registering it in the local and global databases.

        Args:
            artifact_staging_path (Path): The path to the artifact in the staging directory.
            artifact_final_path (Path): The path to move the artifact to in the final directory. 
            Note: The final path must be within the directory specified in the directory argument in the class constructor.

        Returns:
            Tuple[Path, str]: The final path of where the artifact was moved to and its hash.
        """

        if not isinstance(artifact_staging_path, Path):
            raise TypeError("artifact_staging_path must be of type pathlib.Path")

        if not isinstance(artifact_final_path, Path): 
            raise TypeError("artifact_final_path must be of type pathlib.Path")
        
        if not artifact_staging_path.is_relative_to(self.staging_directory):
            raise ValueError(f"Artifact staging path {artifact_staging_path} is not within the staging directory {self.staging_directory}")

        if not artifact_final_path.is_relative_to(self.directory):
            raise ValueError(f"Artifact final path {artifact_final_path} is not within the directory {self.directory}")

        # hash the artifact
        artifact_hash = self.hash_file(artifact_staging_path) if artifact_staging_path.is_file() else self.hash_directory(artifact_staging_path)

        # move the artifact
        artifact_final_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(artifact_staging_path), str(artifact_final_path))

        # register the artifact in the local database
        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
                INSERT OR IGNORE INTO {self.local_table_name} 
                (artifact_path, artifact_hash, node_name, hash_algorithm) 
                VALUES (?, ?, ?, ?);
            """
            local_entry = (str(artifact_final_path), artifact_hash, self.name, "sha256")
            cursor.execute(query, local_entry)
        
        # register the artifact in the global database
        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} 
                (artifact_hash, node_name, run_id, state, details) 
                VALUES (?, ?, ?, ?, ?);
            """
            global_entry = (artifact_hash, self.name, self.run_id, "created", str(artifact_final_path))
            cursor.execute(query, global_entry)
        
        return artifact_final_path, artifact_hash

    def hash_file(self, filepath: str) -> str:
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
