from logging import Logger
import os
import hashlib
from typing import List

from utils.connection import ConnectionManager

sql = str   # alias of the str type for syntax highlighting using the Python Inline Source Syntax Highlighting extension by Sam Willis in VSCode.



class Producer:
    def __init__(self, name: str, directory: str, hash_chunk_size: int = 1_048_576, transports: List = None, logger: Logger = None):
        self.name = name
        self.hash_chunk_size = hash_chunk_size
        self.logger = logger
        self.run_id = 0

        self.transports = transports if transports is not None else []
    
        self.directory = directory
        if os.path.exists(directory) is False:
            self.logger.info(f"Directory {directory} does not exist. Creating it.")
            os.makedirs(directory)

        self.staging_directory = os.path.join(directory, ".staging")
        if os.path.exists(self.staging_directory) is False:
            self.logger.info(f"Temporary directory {self.staging_directory} does not exist. Creating it.")
            os.makedirs(self.staging_directory)

        self.global_usage_table_name = "artifact_usage_events"

        # keep track of artifact paths created in the current run, so that we can log warnings if the same path is being overwritten in the same run
        self.paths_in_current_run = set()   
    
    def get_staging_directory(self) -> str:
        return self.staging_directory
    
    def set_run_id(self, run_id: int):
        self.run_id = run_id

    def initialize_db_connection(self, filename: str):
        self.conn_manager = ConnectionManager(db_path=filename, logger=self.logger)
    
    def restart_producer(self):
        # clear any temp files in the staging directory from previous runs, so that we don't have any leftover temp files when we start a new run
        for filename in os.listdir(self.staging_directory):
            path = os.path.join(self.staging_directory, filename)
            if os.path.isfile(path):
                os.remove(path)
    
    def save_metadata(self, **kwargs) -> dict:
        """
        User implemented method to save custom metadata to be associated with the artifact. 
        The saved metadata will be inserted into the stream's local table when the artifact is detected by the stream.
        The producer and the stream serve as the bookends of the artifact's journey, with the producer responsible for saving the artifact and its metadata, 
        and the stream responsible for loading the artifact and its metadata and making it available to consumers.

        This enables nodes to pass the artifact, its metadata, and its hash to other nodes over the network, ensuring provenance and traceability of artifacts across the entire DAG, even when nodes are running on different machines or in different environments. 

        The user can implement this method to save any custom metadata they want to associate with the artifact, such as hyperparameters used to generate a model artifact, metrics associated with an evaluation artifact, or any other relevant information about the artifact. 
        The saved metadata will then be available in the stream's local table for consumers to query and use as needed.
        """
        pass

    def register_created_artifacts(self) -> None:
        # zip all files in the staging directory and then move it to the producer's directory.
        # once file is transported, DirectoryStream will load in the zipped envelope, extract the metadata from metadata.json,
        # extract the artifact file, load the artifact content, load the artifact hash and other associated metadata from metadata.json, 
        # and then insert the artifact hash and metadata into the stream's local table.

        # user is responsible for overriding save_metadata(**kwargs) method to add custom metadata they want to the metadata.json. 
        # user is also responsible for implementing a load_metadata() method in the DirectoryStream to load the custom metadata from the metadata.json
        # and add the metadata to the stream's local table when the artifact is detected by the stream.
        # essentially, the producer and the stream serve as the bookends of the artifact's journey, 
        # with the producer responsible for saving the artifact and its metadata, 
        # and the stream responsible for loading the artifact and its metadata and making it available to consumers.

        # in the future, this enables nodes to pass the artifact, its metadata, and its hash to other nodes over the network
        # ensuring provenance and traceability of artifacts across the entire DAG, even when nodes are running on different machines or in different environments. 

        for path in os.listdir(self.staging_directory):
            full_path = os.path.join(self.staging_directory, path)
            if os.path.isfile(full_path):
                self.paths_in_current_run.add(full_path)

        entries = []
        for path in self.paths_in_current_run:
            # hash the artifact
            artifact_hash = self.hash_artifact(path)

            # move to final location in the producer's directory
            relative_path = os.path.relpath(path, self.staging_directory)   # /producer_directory/.staging/subdir/filename.txt -> /subdir/filename.txt
            final_path = os.path.join(self.directory, relative_path)        # /producer_directory/subdir/filename.txt
            os.makedirs(os.path.dirname(final_path), exist_ok=True)         # create /producer_directory/subdir if it doesn't exist
            os.replace(path, final_path)                                    # atomic publish to /producer_directory/subdir/filename.txt

            # add entry to local producer table and global usage table with state "created". 
            # The consumer will later update the state to "in_use" when it starts using the artifact and then to "used" when it's done with the artifact.
            entries.append((artifact_hash, self.name, self.run_id, "created", final_path))
            self.logger.info(f"Producer {self.name} registering created artifact {final_path} with hash {artifact_hash} in run {self.run_id}")

        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} 
                (artifact_hash, node_name, run_id, state, details) 
                VALUES (?, ?, ?, ?, ?);
            """
            cursor.executemany(query, entries)
        
        for transport in self.transports:
            transport.send(final_path, artifact_hash)

    def hash_artifact(self, filepath: str) -> str:
        sha256 = hashlib.sha256()
        with open(filepath, 'rb') as f:
            while chunk := f.read(self.hash_chunk_size):
                sha256.update(chunk)
        return sha256.hexdigest()
