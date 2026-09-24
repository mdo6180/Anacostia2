import logging
import os
from logging import Logger
import shutil
from pathlib import Path
from uuid import uuid4
from dataclasses import asdict
import json
from contextlib import contextmanager

from anacostia.utils.logging import log
from anacostia.utils.connection import ConnectionManager
from anacostia.utils.types import Artifact
from anacostia.utils.package import create_deterministic_tar, gzip_file, partition_file, sha256_file
from anacostia.utils.merkle_tree import ProofEntry, generate_proof, merkle_root
from anacostia.utils.chunk import ChunkInfo, TransferArtifact, TransferManifest, ChunkManifest, TransferManifestSignature

sql = str   # alias of the str type for syntax highlighting using the Python Inline Source Syntax Highlighting extension by Sam Willis in VSCode.



class BaseTransport:
    def __init__(
        self, name: str, 
        transfers_directory: Path, 
        hash_chunk_size: int = -1, 
        logger: Logger = None
    ):
        """
        name: name of the Transport
        packages_directory: directory where all of the Transport's packages will be stored.
        hash_chunk_size: size of the chunks to read when hashing files.
            - -1: no chunking, hash the entire file at once (default)
            - 1_048_576: 1 MB
            - 10_485_760: 10 MB
            - 104_857_600: 100 MB
        partition_size: size of the partitions when partitioning files.
        logger: logger for logging statements
        """

        self.name = name
        self.hash_chunk_size = hash_chunk_size
        self.logger = logger
        self.run_id = 0

        # local tables
        self.transfer_artifacts_table = f"{self.name}_local_transfer_artifacts"
        self.transfers_table = f"{self.name}_local_transfers"
        self.chunks_table = f"{self.name}_local_chunks"

        self.transfer_artifacts: list[TransferArtifact] = []  # list of TransferArtifact objects to be added to the transfer package

        # global tables
        self.global_usage_table_name = "artifact_usage_events"

        self.transfers_directory = transfers_directory
        if not self.transfers_directory.exists():
            os.makedirs(self.transfers_directory)

    def set_pipeline_name(self, pipeline_name: str):
        self.pipeline_name = pipeline_name

    def set_db_folder(self, db_folder: str):
        self.db_folder = db_folder
        self.db_path = self.db_folder / "anacostia.db"
        
    def set_node_name(self, node_name: str):
        self.node_name = node_name
        
    def set_run_id(self, run_id: int):
        self.run_id = run_id

    def initialize_db_connection(self, filename: str):
        self.conn_manager = ConnectionManager(db_path=filename, logger=self.logger)
    
    def setup(self):
        """
        Create the local tables for this transport to track transfers, artifacts, and chunks.
        User implemented method (call super().setup() if overriding).
        To add additional tables, user can execute CREATE TABLE statements in their overridden setup() method.
        Make sure table names are unique to avoid conflicts with other transports and producers. 
        We recommend using the convention of prefixing table names with the transport or producer name, 
        e.g. f"{transport_name}_artifacts" for a transport's local table to track artifacts.

        sample implementation:

        ```
        def setup(self):
            super().setup()  # call the base class setup to create the local tables for this transport

            with self.conn_manager.write_cursor() as cursor:
                query: sql = f'''
                CREATE TABLE IF NOT EXISTS {self.name}_metadata (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    key TEXT NOT NULL,
                    value TEXT NOT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                '''
                cursor.execute(query)
        ```
        """

        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
            CREATE TABLE IF NOT EXISTS {self.transfers_table} (
                manifest_hash TEXT PRIMARY KEY,
                manifest_signature TEXT NOT NULL,
                manifest TEXT NOT NULL,
                transfer_id TEXT GENERATED ALWAYS AS (
                    json_extract(manifest, '$.transfer_id')
                ) STORED UNIQUE,
                archive_size INTEGER GENERATED ALWAYS AS (
                    json_extract(manifest, '$.archive_size')
                ) STORED,
                chunk_count INTEGER GENERATED ALWAYS AS (
                    json_extract(manifest, '$.chunk_count')
                ) STORED,
                previous_transfer_id TEXT GENERATED ALWAYS AS (
                    json_extract(manifest, '$.previous_transfer_id')
                ) STORED,
                previous_manifest_hash TEXT GENERATED ALWAYS AS (
                    json_extract(manifest, '$.previous_manifest_hash')
                ) STORED,
                transport_name TEXT NOT NULL,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            );
            """
            cursor.execute(query)

            query: sql = f"""
            CREATE TABLE IF NOT EXISTS {self.transfer_artifacts_table} (
                artifact_hash TEXT NOT NULL,
                manifest_hash TEXT NOT NULL,
                filepath TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                source_pipeline_name TEXT NOT NULL,
                destination_pipeline_name TEXT NOT NULL,
                destination_stream TEXT NOT NULL,
                PRIMARY KEY (artifact_hash, manifest_hash),
                FOREIGN KEY (manifest_hash) REFERENCES {self.transfers_table}(manifest_hash) ON DELETE CASCADE
            );
            """
            cursor.execute(query)

            query: sql = f"""
            /* Trigger to automatically insert transfer artifacts when a new transfer is added */
            CREATE TRIGGER insert_transfer_artifacts
            AFTER INSERT ON {self.transfers_table}
            BEGIN
                INSERT INTO {self.transfer_artifacts_table} (
                    artifact_hash,
                    filepath,
                    size_bytes,
                    source_pipeline_name,
                    destination_pipeline_name,
                    destination_stream,
                    manifest_hash
                )
                SELECT
                    json_extract(artifact.value, '$.artifact_hash'),
                    json_extract(artifact.value, '$.filepath'),
                    json_extract(artifact.value, '$.size_bytes'),
                    json_extract(artifact.value, '$.source_pipeline_name'),
                    json_extract(artifact.value, '$.destination_pipeline_name'),
                    json_extract(artifact.value, '$.destination_stream'),
                    NEW.manifest_hash
                FROM json_each(NEW.manifest, '$.transfer_artifacts') AS artifact;
            END;
            """
            cursor.execute(query)

            query: sql = f"""
            CREATE TABLE IF NOT EXISTS {self.chunks_table} (
                chunk_hash TEXT GENERATED ALWAYS AS (
                    json_extract(chunk_json, '$.chunk_hash')
                ) STORED,
                chunk_index INTEGER NOT NULL,
                chunk_json TEXT NOT NULL,
                manifest_hash TEXT NOT NULL,
                filename TEXT GENERATED ALWAYS AS (
                    json_extract(chunk_json, '$.filename')
                ) STORED,
                offset_bytes INTEGER GENERATED ALWAYS AS (
                    json_extract(chunk_json, '$.offset_bytes')
                ) STORED,
                size_bytes INTEGER GENERATED ALWAYS AS (
                    json_extract(chunk_json, '$.size_bytes')
                ) STORED,
                merkle_root TEXT GENERATED ALWAYS AS (
                    json_extract(chunk_json, '$.merkle_root')
                ) STORED,
                merkle_leaf_index INTEGER GENERATED ALWAYS AS (
                    json_extract(chunk_json, '$.merkle_leaf_index')
                ) STORED,
                merkle_proof TEXT GENERATED ALWAYS AS (
                    json_extract(chunk_json, '$.merkle_proof')
                ) STORED,
                PRIMARY KEY (chunk_index, manifest_hash),   /* chunk index is part of the primary key because two chunks can have the same hash (same content) but different indices */
                FOREIGN KEY (manifest_hash) REFERENCES {self.transfers_table}(manifest_hash) ON DELETE CASCADE
            );
            """
            cursor.execute(query)

    def record_transfer(self, manifest_hash: str, manifest_signature: str, manifest: str):
        """
        Record a transfer in the local transfer table.
        """
        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
            INSERT INTO {self.transfers_table} (manifest_hash, manifest_signature, manifest, transport_name)
            VALUES (?, ?, ?, ?);
            """
            cursor.execute(query, (manifest_hash, manifest_signature, manifest, self.name))

    '''
    def record_chunks(self, transfer_manifest_hash: str, chunks: list[ChunkManifest]):
        """
        Record chunks in the local chunks table.

        transfer_manifest_hash: hash of the transfer manifest that these chunks belong to
        chunks: list of ChunkManifest objects to record
        """

        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
            INSERT INTO {self.chunks_table} (chunk_index, chunk_json, manifest_hash)
            VALUES (?, ?, ?);
            """
            cursor.executemany(
                query, 
                [
                    (chunk.index, json.dumps(chunk), transfer_manifest_hash) 
                    for chunk in chunks
                ]
            )
    '''

    @contextmanager
    def create_transfer_package(self, compression_level: int = 6, partition_size: int = 1_048_576):
        """
        Context manager to create a package for the given artifact.
        Yields the path to the /data folder where all the files for the transfer package should be placed.
        Copy the artifact file into this folder, and when the context is exited, the package will be finalized (e.g., zipped, hashed, and partitioned).

        Args:
            compression_level (int): The level of compression to use when creating the .tar.gz file
            partition_size (int): The size of the partitions to create when partitioning the .tar
        """

        # Logic to create a folder for the transfer package inside the destination directory,
        # create the /data folder, and yield the path to it.
        transfer_id = f"transfer_{uuid4().hex}"
        package_path = self.transfers_directory / transfer_id
        log(message=f"Creating transfer package at '{package_path}'", level="info", logger=self.logger)

        self.data_folder_path = package_path / "data"
        self.data_folder_path.mkdir(parents=True, exist_ok=True)

        # Yield the path to the /data folder to the user
        yield self.data_folder_path    

        # create transfer package once user is done adding files to the package
        try:
            # Creating tar file from the /data folder
            # transfer_7bs43.../data -> transfer_7bs43.../data.tar
            tar_path = package_path / "data.tar"
            tar_path = create_deterministic_tar(self.data_folder_path, tar_path)

            # create the transfer manifest
            transfer_manifest = TransferManifest(
                transfer_id=transfer_id,
                transfer_artifacts=self.transfer_artifacts,
                archive_size=sum(artifact.size_bytes for artifact in self.transfer_artifacts),
                archive_sha256=f"some_hash_{uuid4().hex}",  # Placeholder, will be updated after creating the archive
                chunk_count=0,                  # will be updated after partitioning
                previous_transfer_id=None,      # can be set if needed
                previous_transfer_sha256=None   # can be set if needed
            )

            # create the transfer_manifest.json file
            transfer_manifest_path = package_path / "transfer_manifest.json"
            with transfer_manifest_path.open("x", encoding="utf-8") as manifest_file:
                json.dump(
                    {
                        **asdict(transfer_manifest),
                        "transfer_artifacts": [asdict(artifact) for artifact in self.transfer_artifacts],
                    },
                    manifest_file,
                    indent=4,
                )
                manifest_file.write("\n")

            self.record_transfer(
                manifest_hash=sha256_file(transfer_manifest_path),
                manifest_signature=f"some_signature_{uuid4().hex}",  # Placeholder for actual signature
                manifest=json.dumps(asdict(transfer_manifest))
            )

            # Copy database file to the package directory
            self.conn_manager.copy_database(package_path / "anacostia.db")

        finally:
            # Clean up if necessary
            pass
    
    def add_to_package(self, artifact_hash: str, src_path: Path, dest_path: Path, dest_pipeline_name: str, dest_stream: str) -> Artifact:
        """
        Add an artifact to the transfer package by moving it from the staging directory to the final directory,
        hashing it, and registering it in the local and global databases.

        Args:
            artifact_hash (str): The hash of the artifact to be added to the package.
            src_path (Path): The path to the artifact in the staging directory.
            dest_path (Path): The path to move the artifact to in the package's /data directory. 
            dest_stream (str): The name of the destination stream.
            Note: The final path must be within the directory specified in the directory argument in the class constructor.
            Note: The final path must be within the directory specified in the directory argument in the class constructor.

        Returns:
            Artifact: The committed artifact object where Artifact(location={"path": str(dest_path)}, hash=artifact_hash).
        """

        if not isinstance(src_path, Path):
            raise TypeError("src_path must be of type pathlib.Path")

        if not isinstance(dest_path, Path): 
            raise TypeError("dest_path must be of type pathlib.Path")
        
        if not dest_path.is_relative_to(self.transfers_directory):
            raise ValueError(f"Destination path {dest_path} is not within the directory {self.transfers_directory}")

        # move the artifact
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(src_path), str(dest_path))

        filepath = dest_path.relative_to(self.data_folder_path).as_posix()  # Store the relative path to the transfers directory

        self.transfer_artifacts.append(TransferArtifact(
            artifact_hash=artifact_hash,
            filepath=filepath,
            size_bytes=dest_path.stat().st_size,
            source_pipeline_name=self.pipeline_name,
            destination_pipeline_name=dest_pipeline_name,
            destination_stream=dest_stream
        ))

