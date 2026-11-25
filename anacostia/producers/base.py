import json
from logging import Logger
import os
import hashlib
from typing import Any, Tuple
from pathlib import Path
import shutil

from anacostia.utils.connection import ConnectionManager
from anacostia.utils.logging import log
from anacostia.utils.types import Artifact, JsonDict
from anacostia.utils.hashing import hash_file

sql = str   # alias of the str type for syntax highlighting using the Python Inline Source Syntax Highlighting extension by Sam Willis in VSCode.



class Producer:
    def __init__(self, name: str, commit_directory: Path, logger: Logger = None):
        """
        Initialize a Producer instance.
        
        Args:
            name (str): The name of the producer.
            commit_directory (Path): The directory where artifacts will be stored locally.
            logger (Logger, optional): A logger instance for logging messages. Defaults to None.
        """

        self.name = name
        self.logger = logger
        self.run_id = 0

        self.directory = commit_directory
        if not self.directory.exists():
            log(f"Directory {self.directory} does not exist. Creating it.", level="info", logger=self.logger)
            self.directory.mkdir(parents=True, exist_ok=True)

        self.global_usage_table_name = "artifact_usage_events"
        self.local_table_name = f"{self.name}_local"

        self.node_name = None
        self.pipeline_name = None

        # keep track of artifact paths created in the current run, so that we can log warnings if the same path is being overwritten in the same run
        self.paths_in_current_run = set()   
    
    def setup(self):
        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
                CREATE TABLE IF NOT EXISTS {self.local_table_name} (
                    artifact_hash TEXT PRIMARY KEY,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    artifact_location TEXT NOT NULL CHECK (json_valid(artifact_location)),
                    metadata TEXT CHECK (metadata IS NULL OR json_valid(metadata)),
                    content ANY DEFAULT NULL,
                    UNIQUE(artifact_location)
                );
            """
            cursor.execute(query)

    def set_db_folder(self, db_folder: Path):
        self.db_folder: Path = db_folder
    
    def set_node_name(self, node_name: str):
        self.node_name = node_name
        
    def set_pipeline_name(self, pipeline_name: str):
        self.pipeline_name = pipeline_name

    def get_commit_directory(self) -> Path:
        return self.directory
    
    def set_run_id(self, run_id: int):
        self.run_id = run_id

    def initialize_db_connection(self, filename: str):
        self.conn_manager = ConnectionManager(db_path=filename, logger=self.logger)
    
    def register_artifact(self, artifact: Artifact, metadata: JsonDict = None, content: Any = None):
        """
        Register an artifact in the local and global database tables.
        Implement your own logic to handle the artifact registration (hashing, storing, etc.) and then call this method to register the artifact.

        Args:
            artifact (Artifact): The artifact to register.
            metadata (JsonDict, optional): Optional metadata associated with the artifact. Defaults to None.
            content (Any, optional): Optional content associated with the artifact. Defaults to None.

        **Usage**:
        ```
        class MyProducer(Producer):
            def register_artifact(
                self, file_path: Path, metadata: JsonDict = None, content: Any = None
            ):
                artifact_hash = hash_file(file_path)
                artifact_location = json.dumps({"path": str(file_path)})
                artifact = Artifact(hash=artifact_hash, location=artifact_location)
                super().register_artifact(artifact, metadata, content)
        ```
        """

        artifact_hash = artifact.hash
        artifact_location = json.dumps(artifact.location)
        metadata_json = json.dumps(metadata) if metadata is not None else None

        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
                INSERT OR REPLACE INTO {self.local_table_name} (artifact_hash, artifact_location, metadata, content)
                VALUES (?, ?, ?, ?);
            """
            cursor.execute(query, (artifact_hash, artifact_location, metadata_json, content))

        # register the artifact in the global database
        self.conn_manager.insert_run_event(
            artifact_hash=artifact_hash,
            node_name=self.name,
            pipeline_name=self.pipeline_name,
            run_id=self.run_id,
            state="created",
            details=artifact_location
        )
        
        # add a provenance edge from the node to the producer that produced the artifact
        self.conn_manager.add_provenance_edge(
            predecessor_name=self.node_name, predecessor_type="node",
            successor_name=self.name, successor_type="producer",
            artifact_location=artifact_location,
            artifact_hash=artifact_hash,
            run_id=self.run_id,
        )