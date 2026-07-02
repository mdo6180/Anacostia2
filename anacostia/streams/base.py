import hashlib
import logging
from typing import Any
import json

from anacostia.utils.connection import ConnectionManager
from anacostia.utils.types import JsonDict

sql = str   # alias of the str type for syntax highlighting using the Python Inline Source Syntax Highlighting extension by Sam Willis in VSCode.



class Stream:
    def __init__(self, name: str, source: Any, poll_interval: float = 0.1, logger: logging.Logger = None):
        """
        Base class for streams. Subclasses should implement the __iter__ method to define how the stream polls the source for new artifacts.

        :param name: Name of the stream.
        :param source: The source from which the stream will poll for new artifacts.
        :param poll_interval: The interval (in seconds) at which the stream polls the source for new artifacts.
        :param logger: Logger instance for logging.
        """
        self.name = name
        self.conn_manager: ConnectionManager = None
        self.source = source
        self.poll_interval = poll_interval
        self.logger = logger

        self.local_table_name = f"{self.name}_local"
        self.global_usage_table_name = "artifact_usage_events"
        self.provenance_graph_table_name = "provenance_graph"

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
                    artifact_hash TEXT PRIMARY KEY,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    artifact_location TEXT NOT NULL CHECK (json_valid(artifact_location)),
                    metadata TEXT CHECK (metadata IS NULL OR json_valid(metadata)),
                    UNIQUE(artifact_hash)
                );
            """
            cursor.execute(query)

    def register_artifact(self,  artifact_hash: str, artifact_location: JsonDict, metadata: JsonDict = None) -> None:
        """
        Register an artifact in the stream's local database table and the global database table.

        :param artifact_hash: The hash of the artifact.
        :param artifact_location: The location of the artifact (e.g., file path, URL) formatted as a JSON dictionary.
        :param metadata: Additional metadata to be stored in the stream's local database.
        """
        self.register_artifact_local(artifact_hash, artifact_location, metadata=metadata)
        self.register_artifact_global(artifact_hash)

    def register_artifact_local(self, artifact_hash: str, artifact_location: JsonDict, metadata: JsonDict = None) -> None:
        """
        Register an artifact in the stream's local database table and the global database table. 

        :param artifact_location: The location of the artifact (e.g., file path, URL) formatted as a JSON dictionary.
        :param artifact_hash: The hash of the artifact.
        :param metadata: Additional metadata about the artifact to be stored in the stream's local database formatted as a JSON string.

        Example usage:
        ```
        stream = DirectoryStream(name="example_stream", directory=Path("/path/to/directory"), logger=logger)
        stream.register_artifact_local(
            artifact_hash, 
            artifact_location=json.dumps({"filepath": "/path/to/file.txt"}),
            metadata=json.dumps({"example_metadata": example_metadata})
        )
        ```

        example_tag will be stored in the local table under a column named "tag" and example_metadata will be stored in the local table under a column named "metadata".
        """

        query: sql = f"""
            INSERT OR IGNORE INTO {self.local_table_name} ('artifact_hash', 'artifact_location', 'metadata')
            VALUES (?, ?, ?);
        """
        with self.conn_manager.write_cursor() as cursor:
            cursor.execute(query, (artifact_hash, json.dumps(artifact_location), metadata))

    def register_artifact_global(self, artifact_hash: str) -> None:
        artifact_path = self.get_artifact_location(artifact_hash)
        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} 
                (artifact_hash, node_name, state, details) 
                VALUES (?, ?, ?, ?);
            """
            cursor.execute(query, (artifact_hash, self.name, "detected", artifact_path))
            # self.logger.info(f"Registered artifact {filepath} with hash {artifact_hash} in stream {self.name} at {timestamp}")

    def get_artifact_location(self, artifact_hash: str) -> JsonDict:
        """
        This method should be implemented by subclasses to define how to retrieve the location of an artifact from the local database table given its hash.

        :param artifact_hash: The hash of the artifact.

        :return artifact location: The location of the artifact (e.g., file path, URL) as a JSON dictionary.
        """
        with self.conn_manager.read_cursor() as cursor:
            query: sql = f"""
                SELECT artifact_location FROM {self.local_table_name} WHERE artifact_hash = ? LIMIT 1;
            """
            cursor.execute(query, (artifact_hash,))
            result = cursor.fetchone()
            if result is None:
                raise ValueError(f"Artifact with hash {artifact_hash} not found in local stream table.")
            return json.loads(result[0])

    def is_artifact_registered(self, artifact_location: JsonDict) -> bool:
        """
        Check if an artifact is registered in the stream's local database table based on its location.

        :param artifact_location: The location of the artifact (e.g., file path, URL) formatted as a JSON dictionary.

        :return: True if the artifact is registered, False otherwise.
        """
        with self.conn_manager.read_cursor() as cursor:
            query: sql = f"""
                SELECT 1 FROM {self.local_table_name} WHERE artifact_location = ? LIMIT 1;
            """
            cursor.execute(query, (json.dumps(artifact_location),))
            return cursor.fetchone() is not None

    def hash_artifact(self, artifact: bytes) -> str:
        """
        Hash the artifact using the specified hash algorithm and return the hash value.
        For larger artifacts, override this method with a more efficient hashing strategy to avoid loading the entire artifact into memory.

        :param artifact: The artifact content as bytes to be hashed.

        :return artifact hash: The SHA-256 hash value of the artifact as a string.
        
        Example usage:
        ```
        artifact_content = b"example artifact content"
        artifact_hash = Stream.hash_artifact(artifact_content)

        artifact_content = stream.load_artifact(artifact_location)
        artifact_hash = Stream.hash_artifact(artifact_content)
        ```
        """
        return hashlib.sha256(artifact).hexdigest()
    
    def load_artifact(self, artifact_location: JsonDict) -> bytes:
        """
        Load and return the content of the artifact as bytes given its location. User implemented method.

        :param artifact_location: The location of the artifact (e.g., file path, URL) as a JSON dictionary.

        :return artifact content: The content of the artifact as bytes.

        Example usage:
        ```python
        def load_artifact(self, artifact_location: JsonDict) -> bytes:
            # Example implementation for loading an artifact from a file path
            # Suppose artifact_location = {"filepath": "/path/to/file.txt"}
            with open(artifact_location["filepath"], "rb") as f:
                return f.read()
        ```
        """
        raise NotImplementedError("Subclasses must implement the load_artifact method.")

    def __iter__(self) -> Any:
        """
        This method should be implemented by subclasses to define how the stream polls the source for new artifacts.
        """
        raise NotImplementedError("Subclasses must implement the __iter__ method.")