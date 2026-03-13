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
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    hash_algorithm TEXT,
                    UNIQUE(artifact_path, artifact_hash),
                    UNIQUE(artifact_hash)
                );
            """
            cursor.execute(query)

    def set_db_folder(self, db_folder: str):
        self.db_folder = db_folder
        
        self.staging_directory = os.path.join(db_folder, self.name)
        if os.path.exists(self.staging_directory) is False:
            self.logger.info(f"Temporary directory {self.staging_directory} does not exist. Creating it.")
            os.makedirs(self.staging_directory)

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
        
        # on restart, check which files in the producer's directory has not been detected by the transport's destination stream and send/resend it.
        # doing so handles the following two cases: 1) the destination stream has not received it or 2) the producer has not sent it yet before shutting off. 
        for transport in self.transports:
            unsent_artifacts = self.get_unsent_artifacts(dest_stream_name=transport.dest_stream_name)
            self.logger.info(f"Producer {self.name} restarting. Found {len(unsent_artifacts)} unsent artifacts for transport {transport.name}. Resending them.")
            
            for artifact_path, artifact_hash in unsent_artifacts:
                self.register_artifact_send(transport_name=transport.name, filepath=artifact_path, artifact_hash=artifact_hash)
                transport.send(artifact_path)
    
    def register_artifact_send(self, transport_name: str, filepath: str, artifact_hash: str) -> None:
        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} 
                (artifact_hash, node_name, state, details) 
                VALUES (?, ?, ?, ?);
            """
            cursor.execute(query, (artifact_hash, transport_name, "sent", filepath))

        self.logger.info(f"Producer {self.name} sent artifact {filepath} with hash {artifact_hash} via transport {transport_name} in run {self.run_id}")

    def get_unsent_artifacts(self, dest_stream_name: str) -> List[tuple]:
        """
        Get the list of artifacts that have been created by the producer but have not been sent by the transport yet.
        This is used on producer restart to determine which artifacts still need to be sent by the transport.
        """
        with self.conn_manager.read_cursor() as cursor:
            query: sql = f"""
                SELECT artifact_path, artifact_hash FROM {self.local_table_name} 
                WHERE artifact_hash NOT IN (
                    SELECT artifact_hash FROM {self.global_usage_table_name} 
                    WHERE state = 'detected' AND node_name = ?
                );
            """
            cursor.execute(query, (dest_stream_name,))
            return cursor.fetchall()

    def register_created_artifacts(self) -> None:
        entries = []
        for path in os.listdir(self.staging_directory):
            full_path = os.path.join(self.staging_directory, path)
            if os.path.isfile(full_path):
                # hash the artifact
                artifact_hash = self.hash_artifact(full_path)

                # move to final location in the producer's directory
                relative_path = os.path.relpath(full_path, self.staging_directory)   # /producer_directory/.staging/subdir/filename.txt -> /subdir/filename.txt
                final_path = os.path.join(self.directory, relative_path)        # /producer_directory/subdir/filename.txt
                os.makedirs(os.path.dirname(final_path), exist_ok=True)         # create /producer_directory/subdir if it doesn't exist
                os.replace(full_path, final_path)                                    # atomic publish to /producer_directory/subdir/filename.txt

                # add entry to local producer table and global usage table with state "created". 
                # The consumer will later update the state to "in_use" when it starts using the artifact and then to "used" when it's done with the artifact.
                entries.append((artifact_hash, self.name, self.run_id, "created", final_path))
                self.logger.info(f"Producer {self.name} registering created artifact {final_path} with hash {artifact_hash} in run {self.run_id}")

        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
                INSERT OR IGNORE INTO {self.local_table_name} 
                (artifact_path, artifact_hash, hash_algorithm) 
                VALUES (?, ?, ?);
            """
            local_entries = [(final_path, artifact_hash, "sha256") for artifact_hash, _, _, _, final_path in entries]
            cursor.executemany(query, local_entries)
        
        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} 
                (artifact_hash, node_name, run_id, state, details) 
                VALUES (?, ?, ?, ?, ?);
            """
            cursor.executemany(query, entries)
        
        for artifact_hash, _, _, _, final_path in entries:
            for transport in self.transports:
                self.register_artifact_send(transport_name=transport.name, filepath=final_path, artifact_hash=artifact_hash)
                transport.send(final_path)

    def hash_artifact(self, filepath: str) -> str:
        sha256 = hashlib.sha256()
        with open(filepath, 'rb') as f:
            while chunk := f.read(self.hash_chunk_size):
                sha256.update(chunk)
        return sha256.hexdigest()
