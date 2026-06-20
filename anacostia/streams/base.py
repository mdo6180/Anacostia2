import hashlib
import logging
from typing import Any

from anacostia.utils.connection import ConnectionManager
from anacostia.utils.logging import log

sql = str   # alias of the str type for syntax highlighting using the Python Inline Source Syntax Highlighting extension by Sam Willis in VSCode.



class Stream:
    def __init__(self, name: str, source: Any, logger: logging.Logger):
        """
        Base class for streams. Subclasses should implement the __iter__ method to define how the stream polls the source for new artifacts.
        
        :param name: Name of the stream.
        :param source: The source from which the stream will poll for new artifacts.
        :param logger: Logger instance for logging.
        """
        self.name = name
        self.conn_manager: ConnectionManager = None
        self.source = source
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
                    artifact_location TEXT NOT NULL,
                    artifact_hash TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(artifact_location, artifact_hash),
                    UNIQUE(artifact_hash)
                );
            """
            cursor.execute(query)

    def register_artifact(self, artifact_location: str, artifact_hash: str, **kwargs) -> None:
        """
        Register an artifact in the stream's local database table and the global database table.
        :param artifact_location: The location of the artifact (e.g., file path, URL).
        :param artifact_hash: The hash of the artifact.
        :param kwargs: Additional keyword arguments to be stored in the stream's local database (e.g., tags, metadata).
        """
        self.register_artifact_local(artifact_location, artifact_hash, **kwargs)
        self.register_artifact_global(artifact_hash)

    def register_artifact_local(self, artifact_location: str, artifact_hash: str, **kwargs) -> None:
        """
        Register an artifact in the stream's local database table and the global database table. 

        :param artifact_location: The location of the artifact (e.g., file path, URL).
        :param artifact_hash: The hash of the artifact.
        :param kwargs: Additional keyword arguments to be stored in the stream's local database (e.g., tags, metadata).

        Example usage:
        ```
        stream = DirectoryStream(name="example_stream", directory=Path("/path/to/directory"), logger=logger)
        stream.register_artifact_local(artifact_location, artifact_hash, tag="example_tag", metadata="example_metadata")
        ```

        example_tag will be stored in the local table under a column named "tag" and example_metadata will be stored in the local table under a column named "metadata".
        """

        columns = ['artifact_location', 'artifact_hash'] + list(kwargs.keys())
        values = (artifact_location, artifact_hash) + tuple(kwargs.values())
        placeholders = ', '.join(['?'] * len(values))

        query: sql = f"""
            INSERT OR IGNORE INTO {self.local_table_name} ({', '.join(columns)})
            VALUES ({placeholders});
        """

        with self.conn_manager.write_cursor() as cursor:
            cursor.execute(query, values)

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

    def get_artifact_location(self, artifact_hash: str) -> Any:
        """
        This method should be implemented by subclasses to define how to retrieve the location of an artifact given its hash.

        :param artifact_hash: The hash of the artifact.

        :return artifact location: The location of the artifact (e.g., file path, URL).
        """
        raise NotImplementedError("Subclasses must implement the get_artifact_location method.")

    def load_artifact(self, artifact_location: Any) -> bytes:
        """
        Load and return the content of the artifact as bytes given its location. User implemented method.

        :param artifact_location: The location of the artifact (e.g., file path, URL).

        :return artifact content: The content of the artifact as bytes.
        """
        raise NotImplementedError("Subclasses must implement the load_artifact method.")

    @staticmethod
    def hash_artifact(artifact: bytes) -> str:
        """
        Hash the artifact using the specified hash algorithm and return the hash value. User implemented method.

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
    
    def __iter__(self) -> Any:
        """
        This method should be implemented by subclasses to define how the stream polls the source for new artifacts.
        """
        raise NotImplementedError("Subclasses must implement the __iter__ method.")