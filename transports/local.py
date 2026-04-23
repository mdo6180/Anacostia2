from logging import Logger
import os
import shutil
import json
from typing import Tuple
from pathlib import Path
import hashlib

from utils.connection import ConnectionManager

sql = str   # alias of the str type for syntax highlighting using the Python Inline Source Syntax Highlighting extension by Sam Willis in VSCode.


class FileSystemTransport:
    def __init__(self, name: str, packages_directory: str, logger: Logger = None):
        """
        name: name of the Transport
        packages_directory: directory where all of the Transport's packages will be stored.
        logger: logger for logging statements
        """

        self.name = name
        self.logger = logger
        self.run_id = 0
        self.local_table_name = f"{self.name}_local"
        self.global_usage_table_name = "artifact_usage_events"

        self.dest_directory = Path(packages_directory)
        if not self.dest_directory.exists():
            os.makedirs(self.dest_directory)
        
        self.metadata = []
    
    def set_db_folder(self, db_folder: str):
        self.db_folder = db_folder
        
    def initialize_db_connection(self, filename: str):
        self.conn_manager = ConnectionManager(db_path=filename, logger=self.logger)

    def set_run_id(self, run_id: int):
        self.run_id = run_id

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

    def initialize_staging_directory(self):
        self.staging_directory = os.path.join(self.db_folder, self.name)
        if os.path.exists(self.staging_directory) is False:
            self.logger.info(f"Temporary directory {self.staging_directory} does not exist. Creating it.")
            os.makedirs(self.staging_directory)

    def get_staging_directory(self) -> Path:
        return Path(self.staging_directory)
    
    def stage_artifact(self, artifact_path: Path, artifact_staging_path: Path, artifact_hash: str) -> Path:
        """
        Stage an artifact in the transport's staging directory.

        Args:
            artifact_path (Path): The path to the artifact.
            artifact_staging_path (Path): The path to copy the artifact to in the transport's staging directory. 
            Note: The final path must be within the directory specified in the directory argument in the class constructor.

        Returns:
            Tuple[Path, str]: The final path of where the artifact was moved to and its hash.
        """

        if not isinstance(artifact_path, Path):
            raise TypeError("artifact_path must be of type pathlib.Path")
        
        if not artifact_path.exists():
            raise ValueError(f"Artifact path {artifact_path} does not exist")

        if not isinstance(artifact_staging_path, Path): 
            raise TypeError("artifact_staging_path must be of type pathlib.Path")

        if not artifact_staging_path.is_relative_to(self.staging_directory):
            raise ValueError(f"Artifact final path {artifact_staging_path} is not within the staging directory {self.staging_directory}")

        # recreate the staging directory and create the parent directories if they don't exist
        artifact_staging_path.parent.mkdir(parents=True, exist_ok=True)
        
        # move the artifact
        shutil.copy2(artifact_path, artifact_staging_path)

        relative_path = artifact_staging_path.relative_to(self.staging_directory)
        self.metadata.append({
            "filename": str(relative_path),
            "hash": artifact_hash
        })
    
    def package(self) -> Tuple[Path, str]:
        # create the metadata file
        with open(self.get_staging_directory() / "metadata.json", "w") as f:
            json.dump(self.metadata, f, indent=4)
        
        # reset metadata
        self.metadata = []
        
        # create the destination directory
        package_path = self.dest_directory / f"run_{self.run_id}"
        os.makedirs(package_path, exist_ok=True)

        # hash the package
        package_hash = self.hash_directory(package_path)
        
        # rename staging directory to final package directory (this also deletes the staging directory)
        self.get_staging_directory().rename(package_path)

        # register package in global database
        self.register_artifact_packaged(package_path, package_hash)

        return package_path, package_hash
    
    def register_artifact_packaged(self, package_path: Path, package_hash: str) -> None:
        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} 
                (artifact_hash, node_name, state, details) 
                VALUES (?, ?, ?, ?);
            """
            cursor.execute(query, (package_hash, self.name, "packaged", str(package_path)))

    def restart_transport(self):
        self.clear_staging_directory()

    def clear_staging_directory(self):
        # clear any temp files in the staging directory from previous runs, so that we don't have any leftover temp files when we start a new run
        for filename in os.listdir(self.staging_directory):
            path = os.path.join(self.staging_directory, filename)
            if os.path.isfile(path):
                self.logger.warning(f"Transport {self.name} found leftover file {path} in staging directory from previous run. Removing it.")
                os.remove(path)
            elif os.path.isdir(path):
                self.logger.warning(f"Transport {self.name} found leftover directory {path} in staging directory from previous run. Removing it.")
                shutil.rmtree(path)
    
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