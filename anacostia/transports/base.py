import argparse
import logging
import os
from logging import Logger
import shutil
from pathlib import Path
from uuid import uuid4
from dataclasses import asdict
import json

from anacostia.utils.connection import ConnectionManager
from anacostia.utils.debug import attach_debugger
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

        # global tables
        self.global_usage_table_name = "artifact_usage_events"

        self.transfers_directory = transfers_directory
        if not self.transfers_directory.exists():
            os.makedirs(self.transfers_directory)

    def set_db_folder(self, db_folder: str):
        self.db_folder = db_folder
        
    def set_node_name(self, node_name: str):
        self.node_name = node_name
        
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
                node_name TEXT NOT NULL,
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
                chunk_hash TEXT NOT NULL,
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