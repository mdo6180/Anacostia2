import json
import threading
import time
from typing import List
from logging import Logger
from pathlib import Path
import tarfile

from anacostia.streams.base import Stream
from anacostia.node import Stage
from anacostia.utils.connection import ConnectionManager
from anacostia.utils.logging import log

sql = str   # alias of the str type for syntax highlighting using the Python Inline Source Syntax Highlighting extension by Sam Willis in VSCode.



class Graph:
    def __init__(self, name: str, nodes: List[Stage], db_folder: Path = ".anacostia", logger: Logger = None) -> None:
        self.name = name
        self.nodes = nodes
        self.db_folder = db_folder
        self.logger = logger
        self._stop = threading.Event()

        if not self.db_folder.exists():
            self.db_folder.mkdir(parents=True, exist_ok=True)
        
        db_path = self.db_folder / 'anacostia.db'
        if db_path.exists() is True:
            log(f"Database found at {db_path}. Connecting...", level="info", logger=self.logger)

        self.receiving_directory = self.db_folder / 'receiving'
        if not self.receiving_directory.exists():
            self.receiving_directory.mkdir(parents=True, exist_ok=True)
            log(f"Created receiving directory at {self.receiving_directory}.", level="info", logger=self.logger)

        self.staging_directory = self.db_folder / 'staging'
        if not self.staging_directory.exists():
            self.staging_directory.mkdir(parents=True, exist_ok=True)
            log(f"Created staging directory at {self.staging_directory}.", level="info", logger=self.logger)

        self.storage_directory = self.db_folder / 'storage'
        if not self.storage_directory.exists():
            self.storage_directory.mkdir(parents=True, exist_ok=True)
            log(f"Created storage directory at {self.storage_directory}.", level="info", logger=self.logger)

        self.conn_manager = ConnectionManager(db_path, logger=self.logger)
        self.conn_manager.create_global_tables()

        self.streams: List[Stream] = []

        for node in self.nodes:
            # initialize DB connection for each node, its consumers, and producers
            node.set_db_path(db_path)
            node.initialize_db_connection(db_path)
            node.set_db_folder(self.db_folder)
            node.set_staging_directory(self.staging_directory)
            node.setup()

            for consumer in node.consumers:
                consumer.set_db_path(db_path)
                consumer.stream.initialize_db_connection(db_path)
                consumer.stream.setup()
                self.streams.append(consumer.stream)
                
            for producer in node.producers:
                producer.set_db_folder(self.db_folder)
                producer.initialize_db_connection(db_path)
                producer.setup()

            for transport in node.transports:
                transport.set_db_folder(self.db_folder)
                transport.initialize_db_connection(db_path)
                transport.setup()
                transport.set_pipeline_name(self.name)  # Set the pipeline name for the transport to the graph's name

    def monitor_receiving_directory(self):
        log(f"Monitoring receiving directory at {self.receiving_directory}.", level="info", logger=self.logger)
        while self._stop.is_set() is False:
            
            # Assumption: everything in the receiving directory is a tar file that has been transferred from a remote location. 
            # The tar file contains a transfer_manifest.json, a signature.json, and maybe a chunk.json
            for transfer_package in self.receiving_directory.iterdir():
                if transfer_package.is_file() and transfer_package.suffix == ".tar":
                    try:
                        with tarfile.open(transfer_package, "r") as tar:
                            contents = [member.name for member in tar.getmembers()]
                            transfer_id = contents[0]   # first member of the tar file is the name of the transfer folder, which is also the transfer_id

                            if f"{transfer_id}/transfer_manifest.json" not in contents:
                                log(f"Transfer package {transfer_package} does not contain a transfer_manifest.json. Skipping.", level="warning", logger=self.logger)
                                continue

                            if f"{transfer_id}/signature.json" not in contents:
                                log(f"Transfer package {transfer_package} does not contain a signature.json. Skipping.", level="warning", logger=self.logger)
                                continue

                            if f"{transfer_id}/anacostia.db" not in contents:
                                log(f"Transfer package {transfer_package} does not contain anacostia.db. Skipping.", level="warning", logger=self.logger)
                                continue

                            # if no chunk.json file is found in the tar file, then we can assume this transfer package is not a chunked transfer
                            # and we can immediately move it to the desired stream directory.
                            if f"{transfer_id}/chunk.json" not in contents:
                                log(f"Transfer package {transfer_package} does not contain a chunk.json. Moving to stream directory.", level="warning", logger=self.logger)

                            log(f"Extracting transfer package {transfer_package} to storage directory.", level="info", logger=self.logger)
                            tar.extractall(path=self.storage_directory)
                            log(f"Extracted transfer package {transfer_package} to storage directory.", level="info", logger=self.logger)

                    except Exception as e:
                        log(f"Failed to extract transfer package {transfer_package}: {e}", level="error", logger=self.logger)
                        continue

                    # After extracting, we can remove the original tar file
                    transfer_package.unlink()
                    log(f"Removed transfer package {transfer_package} after extraction.", level="info", logger=self.logger)
                
                else:
                    log(f"Skipping non-tar file {transfer_package} in receiving directory.", level="warning", logger=self.logger)

                """
                if chunk_folder.is_dir():
                    transfer_manifest = chunk_folder / "transfer_manifest.json"
                    chunk_manifest = chunk_folder / "chunk_manifest.json"

                    try:
                        with (
                            open(transfer_manifest, "r") as transfer_manifest_file,
                            open(chunk_manifest, "r") as chunk_manifest_file
                        ):
                            transfer_manifest_data = json.load(transfer_manifest_file)
                            transfer_id = transfer_manifest_data["transfer_id"]

                            chunk_manifest_data = json.load(chunk_manifest_file)

                            # Expected SHA-256 hash
                            chunk_sha256 = chunk_manifest_data["chunk_info"]["sha256"]

                    except FileNotFoundError:
                        # Sometimes the chunk binary is so big that it takes the OS some time to copy over 
                        # both the binary and the transfer manifest chunk folder.
                        # Because the chunk takes some time to copy over, the open() command will fail and throw a FileNotFoundError
                        # because the transfer manifest has not been transfered yet.

                        # if the chunk binary has been copied successfully but the transfer manifest still hasn't arrived,
                        # then we need to throw a warning and move onto other packages.
                        # Eventually we will come back to check on this chunk to see if maybe the user has found the transfer manifest.
                        print("Warning: No transfer manifest detected")
                """

            time.sleep(0.1)  # Sleep for a short duration to avoid busy waiting

        log(f"Stopped monitoring receiving directory at {self.receiving_directory}.", level="info", logger=self.logger)

    def start(self):
        log(f"Starting graph '{self.name}' with {len(self.nodes)} nodes.", level="info", logger=self.logger)
        for node in self.nodes:
            node.start()

        self.monitor_thread = threading.Thread(
            name=f"{self.name}-receiving-monitor",
            target=self.monitor_receiving_directory, 
            daemon=True
        )
        self.monitor_thread.start()
    
    def join(self):
        for node in self.nodes:
            node.join()
        self.monitor_thread.join()
    
    def stop(self):
        log(f"Stopping graph '{self.name}' with {len(self.nodes)} nodes.", level="info", logger=self.logger)
        self._stop.set()
        for node in self.nodes:
            node.stop_consumers()
