import hashlib
from logging import Logger
import os
import time
from typing import Any, Generator, Tuple
from pathlib import Path

from anacostia.utils.connection import ConnectionManager



class DirectoryStream:
    def __init__(self, name: str, resource_db_path: Path, poll_interval: float = 0.1, hash_chunk_size: int = 1_048_576, logger: Logger = None):
        self.logger = logger

        self.name = name
        self.resource_db_path: Path = resource_db_path
        self.poll_interval = poll_interval
        self.hash_chunk_size = hash_chunk_size

        self.conn_manager: ConnectionManager = None
        self.local_table_name = f"{self.name}_local"
        self.global_usage_table_name = "artifact_usage_events"
        self.provenance_graph_table_name = "provenance_graph"

    def initialize_db_connection(self, filename: str):
        self.conn_manager = ConnectionManager(db_path=filename, logger=self.logger)

    def set_node_name(self, node_name: str):
        self.name = node_name

    def setup(self):
        pass
    
    def __getitem__(self, hash: str) -> Tuple[str, Any]:
        """
        Get an artifact from the stream based on its hash.
        Returns a tuple of the artifact location and the artifact content.
        Artifact location can be a file path, a URL, or any other identifier that can be used to retrieve the artifact content.
        Artifact content can be the content of a file, the response from an API call, the values in a database row represented as JSON,
        or any other data that can be represented in Python.

        hash: the hash of the artifact to retrieve
        """
        pass