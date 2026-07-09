import logging
from typing import Any, List
from collections.abc import Iterator
import json

from anacostia.utils.connection import ConnectionManager
from anacostia.utils.types import Artifact, JsonDict

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

        This is an example of how to add additional columns to the local table in an overridden setup() method.
        This example uses the data from the artifact_location field to fill in the topic, partition, and offset columns in the local table to track Kafka artifacts.
        You can also do the same thing using the metadata field to store additional information about the artifact, such as tags, labels, or other metadata.
        From here, you can also write additional methods that query the local table to retrieve artifacts based on their topic, partition, offset, or other metadata.
        ```
        CREATE TABLE IF NOT EXISTS {self.local_table_name} (
            artifact_hash TEXT PRIMARY KEY,
            timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
            artifact_location TEXT NOT NULL,
            metadata TEXT,
            topic TEXT GENERATED ALWAYS AS (
                json_extract(
                    artifact_location,
                    '$.topic'
                )
            ) STORED,
            partition INTEGER GENERATED ALWAYS AS (
                json_extract(
                    artifact_location,
                    '$.partition'
                )
            ) STORED,
            offset INTEGER GENERATED ALWAYS AS (
                json_extract(
                    artifact_location,
                    '$.offset'
                )
            ) STORED,
            UNIQUE(artifact_location)
        );
        ```
        """
        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
                CREATE TABLE IF NOT EXISTS {self.local_table_name} (
                    artifact_hash TEXT PRIMARY KEY,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    artifact_location TEXT NOT NULL CHECK (json_valid(artifact_location)),
                    metadata TEXT CHECK (metadata IS NULL OR json_valid(metadata)),
                    UNIQUE(artifact_location)
                );
            """
            cursor.execute(query)

    def register_artifact(self, artifact: Artifact, metadata: JsonDict = None) -> None:
        """
        Register an artifact in the stream's local database table and the global database table.

        :param artifact: The artifact object containing the hash and location.
        :param metadata: Additional metadata to be stored in the stream's local database.
        :param metadata: Additional metadata to be stored in the stream's local database.
        """
        self.register_artifact_local(artifact.hash, artifact.location, metadata=metadata)
        self.register_artifact_global(artifact.hash)

    def register_artifact_local(self, artifact_hash: str, artifact_location: JsonDict, metadata: JsonDict = None) -> None:
        """
        Register an artifact in the stream's local database table. 

        :param artifact_location: The location of the artifact (e.g., file path, URL) formatted as a JSON dictionary.
        :param artifact_hash: The hash of the artifact.
        :param metadata: Additional metadata about the artifact to be stored in the stream's local database formatted as a JSON dictionary.

        Example usage:
        ```
        stream.register_artifact_local(
            artifact_hash, 
            artifact_location={"filepath": "/path/to/file.txt"},
            metadata={"example_metadata": example_metadata}
        )
        ```

        example_tag will be stored in the local table under a column named "tag" and example_metadata will be stored in the local table under a column named "metadata".
        """

        query: sql = f"""
            INSERT OR IGNORE INTO {self.local_table_name} ('artifact_hash', 'artifact_location', 'metadata')
            VALUES (?, ?, ?);
        """
        with self.conn_manager.write_cursor() as cursor:
            cursor.execute(
                query, 
                (
                    artifact_hash, 
                    json.dumps(artifact_location, sort_keys=True), 
                    json.dumps(metadata) if metadata is not None else None,
                )
            )

    def register_artifact_global(self, artifact_hash: str) -> None:
        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} 
                (artifact_hash, node_name, state, details) 
                VALUES (?, ?, ?, ?);
            """
            cursor.execute(query, (artifact_hash, self.name, "detected", None))
            # self.logger.info(f"Registered artifact {filepath} with hash {artifact_hash} in stream {self.name} at {timestamp}")

    def get_artifact_location(self, artifact_hash: str) -> JsonDict:
        """
        Retrieve the location of an artifact from the stream's local database table based on its hash.
        Artifact location is stored as a JSON dictionary in the local table.

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

    def get_all_artifact_locations(self) -> List[JsonDict]:
        """
        Retrieve the locations of all artifacts from the stream's local database table.
        Artifact locations are stored as JSON dictionaries in the local table.

        :return artifact locations: A list of artifact locations (e.g., file paths, URLs) as JSON dictionaries.
        """
        with self.conn_manager.read_cursor() as cursor:
            query: sql = f"""
                SELECT artifact_location FROM {self.local_table_name};
            """
            cursor.execute(query)
            results = cursor.fetchall()
            return [json.loads(result[0]) for result in results]

    def get_artifact_metadata(self, artifact_hash: str) -> JsonDict:
        """
        Retrieve the metadata of an artifact from the stream's local database table based on its hash.
        Artifact metadata is stored as a JSON dictionary in the local table.

        :param artifact_hash: The hash of the artifact.

        :return artifact metadata: The metadata of the artifact as a JSON dictionary. If no metadata is associated with the artifact, returns None.
        """
        with self.conn_manager.read_cursor() as cursor:
            query: sql = f"""
                SELECT metadata FROM {self.local_table_name} WHERE artifact_hash = ? LIMIT 1;
            """
            cursor.execute(query, (artifact_hash,))
            result = cursor.fetchone()
            if result is None:
                raise ValueError(f"Artifact with hash {artifact_hash} not found in local stream table.")
            return json.loads(result[0]) if result[0] is not None else None

    def is_artifact_registered(self, artifact_location: JsonDict) -> bool:
        """
        Check if an artifact is registered in the stream's local database table based on its location.

        :param artifact_location: The location of the artifact (e.g., file path, URL) formatted as a JSON dictionary.

        :return: True if the artifact is registered in the stream's local database table, False otherwise.
        """
        with self.conn_manager.read_cursor() as cursor:
            query: sql = f"""
                SELECT 1 FROM {self.local_table_name} WHERE artifact_location = ? LIMIT 1;
            """
            cursor.execute(query, (json.dumps(artifact_location, sort_keys=True),))
            return cursor.fetchone() is not None

    def __iter__(self) -> Iterator[Artifact]:
        """
        This method should be implemented by subclasses to define how the stream polls the source for new artifacts.

        :return: An iterator that yields Artifact objects as they are detected in the stream.
        """
        raise NotImplementedError("Subclasses must implement the __iter__ method.")