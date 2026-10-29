import json
import shutil
import threading
import time
from typing import List
from logging import Logger
from pathlib import Path
import tarfile
import os

from anacostia.streams.base import Stream
from anacostia.streams.filesystem import DirectoryStream
from anacostia.node import Stage
from anacostia.utils.connection import ConnectionManager, AttachedConnectionManager
from anacostia.utils.logging import log
from anacostia.utils.hashing import hash_file_object
from anacostia.utils.extraction import inspect_tar_gz, extract_tar_gz

sql = str   # alias of the str type for syntax highlighting using the Python Inline Source Syntax Highlighting extension by Sam Willis in VSCode.



class Graph:
    def __init__(self, name: str, nodes: List[Stage], db_folder: Path = ".anacostia", logger: Logger = None) -> None:
        self.name = name
        self.nodes = nodes
        self.db_folder = db_folder
        self.logger = logger
        self._stop = threading.Event()

        self.global_usage_table_name = "artifact_usage_events"
        self.provenance_graph_table_name = "provenance_graph"

        if not self.db_folder.exists():
            self.db_folder.mkdir(parents=True, exist_ok=True)
        
        self.db_path = self.db_folder / 'anacostia.db'
        if self.db_path.exists() is True:
            log(f"Database found at {self.db_path}. Connecting...", level="info", logger=self.logger)

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

        self.conn_manager = ConnectionManager(self.db_path, logger=self.logger)
        self.conn_manager.create_global_tables()

        self.streams: List[Stream] = []

        for node in self.nodes:
            # initialize DB connection for each node, its consumers, and producers
            node.set_pipeline_name(self.name)  # Set the pipeline name for the node to the graph's name
            node.set_db_path(self.db_path)
            node.initialize_db_connection(self.db_path)
            node.set_db_folder(self.db_folder)
            node.set_staging_directory(self.staging_directory)
            node.setup()

            for consumer in node.consumers:
                consumer.set_pipeline_name(self.name)
                consumer.set_db_path(self.db_path)
                consumer.stream.set_pipeline_name(self.name)
                consumer.stream.initialize_db_connection(self.db_path)
                consumer.stream.setup()
                self.streams.append(consumer.stream)
                
            for producer in node.producers:
                producer.set_pipeline_name(self.name)
                producer.set_db_folder(self.db_folder)
                producer.initialize_db_connection(self.db_path)
                producer.setup()

            for transport in node.transports:
                transport.set_db_folder(self.db_folder)
                transport.initialize_db_connection(self.db_path)
                transport.setup()
                transport.set_pipeline_name(self.name)  # Set the pipeline name for the transport to the graph's name

    def monitor_receiving_directory(self):
        log(f"Monitoring receiving directory at {self.receiving_directory}.", level="info", logger=self.logger)
        while self._stop.is_set() is False:
            
            # Assumption: everything in the receiving directory is a tar file that has been transferred from a remote location. 
            # The tar file contains a transfer_manifest.json, a signature.json, and maybe a chunk.json
            for transfer_package in self.receiving_directory.iterdir():
                if transfer_package.is_file() and transfer_package.suffix == ".tar" and transfer_package.name.startswith("transfer_"):
                    try:
                        with tarfile.open(transfer_package, "r") as tar:
                            contents = [Path(member.name) for member in tar.getmembers()]
                            transfer_id = contents[0]   # first member of the tar file is the name of the transfer folder, which is also the transfer_id

                            if transfer_id / "transfer_manifest.json" not in contents:
                                log(f"Transfer package {transfer_package} does not contain a transfer_manifest.json. Skipping.", level="warning", logger=self.logger)
                                continue

                            if transfer_id / "signature.json" not in contents:
                                log(f"Transfer package {transfer_package} does not contain a signature.json. Skipping.", level="warning", logger=self.logger)
                                continue

                            if transfer_id / "anacostia.db" not in contents:
                                log(f"Transfer package {transfer_package} does not contain anacostia.db. Skipping.", level="warning", logger=self.logger)
                                continue

                            # if no chunk.json file is found in the tar file, then we can assume this transfer package is not a chunked transfer
                            # and we can immediately move it to the desired stream directory.
                            if transfer_id / "chunk.json" not in contents:
                                log(f"Transfer package {transfer_package} does not contain a chunk.json. Moving to stream directory.", level="warning", logger=self.logger)

                            with (
                                tar.extractfile(str(transfer_id / "transfer_manifest.json")) as manifest_file, 
                                tar.extractfile(str(transfer_id / "signature.json")) as signature_file
                            ):
                                manifest_signature = json.load(signature_file)
                                expected_sha256 = manifest_signature['manifest_hash']
                                if expected_sha256 is None:
                                    log(f"Transfer package {transfer_package} does not contain a SHA-256 hash in signature.json. Skipping.", level="warning", logger=self.logger)
                                    continue
                                
                                # Calculate the SHA-256 hash of the transfer_manifest.json
                                actual_sha256 = hash_file_object(manifest_file)
                                if actual_sha256 != expected_sha256:
                                    log(f"SHA-256 hash mismatch for transfer package {transfer_package}. Expected: {expected_sha256}, Actual: {actual_sha256}. Skipping.", level="error", logger=self.logger)
                                    continue

                                # Rewind the stream pointer back to the start of the file
                                manifest_file.seek(0)

                                manifest = json.load(manifest_file)
                                destination_pipeline_name = manifest["destination_pipeline_name"]
                                if destination_pipeline_name != self.name:
                                    log(f"Destination pipeline '{destination_pipeline_name}' does not match current graph name '{self.name}'. Skipping.", level="error", logger=self.logger)
                                    continue

                                # If the transfer package is valid, we can extract it to the staging directory and record its usage in the global_usage table.
                                self.conn_manager.insert_run_event(
                                    artifact_hash=actual_sha256,
                                    node_name=self.name,
                                    pipeline_name=self.name,
                                    state="received",
                                    details={"transfer_package": str(transfer_package.name)}
                                )

                            log(f"Extracting transfer package {transfer_package} to staging directory.", level="info", logger=self.logger)
                            tar.extractall(path=self.staging_directory)
                            log(f"Extracted transfer package {transfer_package} to staging directory.", level="info", logger=self.logger)

                            self.conn_manager.insert_run_event(
                                artifact_hash=actual_sha256,
                                node_name=self.name,
                                pipeline_name=self.name,
                                state="unpackaged",
                                details={"transfer_package": str(transfer_package.name)}
                            )

                    except Exception as e:
                        log(f"Failed to extract transfer package {transfer_package}: {e}", level="error", logger=self.logger)
                        continue

                    # After extracting, we can remove the original tar file
                    transfer_package.unlink()
                    log(f"Removed transfer package {transfer_package} after extraction.", level="info", logger=self.logger)
                
                else:
                    log(f"Skipping non-tar file {transfer_package} in receiving directory.", level="warning", logger=self.logger)

            time.sleep(0.2)  # Sleep for a short duration to avoid busy waiting

        log(f"Stopped monitoring receiving directory at {self.receiving_directory}.", level="info", logger=self.logger)

    def monitor_staging_directory(self):
        log(f"Monitoring staging directory at {self.staging_directory}.", level="info", logger=self.logger)
        while self._stop.is_set() is False:

            for transfer_folder in self.staging_directory.iterdir():
                if transfer_folder.is_dir() and transfer_folder.name.startswith("transfer_"):
                    incoming_db_path = transfer_folder / "anacostia.db"
                    alias = "incoming"

                    with AttachedConnectionManager(self.db_path, logger=self.logger) as attached_connection_manager:
                        attached_connection_manager.import_incoming_db(alias, incoming_db_path)

                    data_compress_path = transfer_folder / "data.tar.gz"

                    # inspect data.tar.gz file using the information in transfer_manifest.json before extracting it to the transfer folder.
                    manifest_path = transfer_folder / "transfer_manifest.json"
                    with open(manifest_path, "r") as manifest_file:
                        manifest = json.load(manifest_file)

                        expected_archive_size = manifest["archive_size"]
                        actual_archive_size = data_compress_path.stat().st_size
                        if actual_archive_size != expected_archive_size:
                            log(f"Size mismatch for data.tar.gz in transfer folder {transfer_folder}. Expected: {expected_archive_size}, Actual: {actual_archive_size}. Skipping.", level="error", logger=self.logger)
                            continue

                        expected_archive_sha256 = manifest["archive_sha256"]
                        with open(data_compress_path, "rb") as f:
                            actual_archive_sha256 = hash_file_object(f)
                            if actual_archive_sha256 != expected_archive_sha256:
                                log(f"SHA-256 hash mismatch for data.tar.gz in transfer folder {transfer_folder}. Expected: {expected_archive_sha256}, Actual: {actual_archive_sha256}. Skipping.", level="error", logger=self.logger)
                                continue

                        streams = [stream.name for stream in self.streams]
                        transfer_artifacts = manifest["transfer_artifacts"]

                        for artifact in transfer_artifacts:
                            destination_stream_name = artifact["destination_stream"]
                            if destination_stream_name not in streams:
                                log(f"Destination stream '{destination_stream_name}' for artifact '{artifact['artifact_id']}' not found in graph streams. Skipping.", level="error", logger=self.logger)
                                continue

                        # inspect the data.tar.gz file to ensure it meets the security policy and size limits before extracting it.
                        inspected_entries = inspect_tar_gz(data_compress_path)

                        # uncompress the data.tar.gz file into the transfer folder and remove the data.tar.gz file
                        data_folder = transfer_folder / "data"
                        extract_tar_gz(data_compress_path, data_folder)

                        os.remove(data_compress_path)

                        # extract_tar_gz unpacked the data.tar.gz file into a "data" folder inside the transfer folder. 
                        # The artifacts are now located in transfer_folder/data/data
                        for artifact_entry in transfer_artifacts:
                            artifact_location = data_folder / "data" / artifact_entry["filepath"]

                            for stream in self.streams:
                                if stream.name == artifact_entry["destination_stream"]:
                                    if not isinstance(stream, DirectoryStream):
                                        log(f"Stream '{stream.name}' is not a DirectoryStream. Cannot add artifact '{artifact_entry['artifact_id']}' to it.", level="error", logger=self.logger)
                                        continue
                                    else:
                                        stream_folder = stream.directory
                                        destination_path = stream_folder / artifact_entry["filepath"]
                                        destination_path.parent.mkdir(parents=True, exist_ok=True)
                                        artifact_location.rename(destination_path)
                                        log(f"Moved artifact '{artifact_entry['filepath']}' to stream '{stream.name}' from transfer folder '{transfer_folder}'.", level="info", logger=self.logger)
                                        break

                        # remove data folder after moving all artifacts to their respective streams
                        shutil.rmtree(data_folder)

                        # remove sqlite database file after moving all artifacts to their respective streams
                        os.remove(incoming_db_path)

                        # move tranfer folder to storage directory after moving all artifacts to their respective streams
                        storage_transfer_folder = self.storage_directory / transfer_folder.name
                        transfer_folder.rename(storage_transfer_folder)

            time.sleep(0.2)  # Sleep for a short duration to avoid busy waiting

        log(f"Stopped monitoring staging directory at {self.staging_directory}.", level="info", logger=self.logger)

    def start(self):
        log(f"Starting graph '{self.name}' with {len(self.nodes)} nodes.", level="info", logger=self.logger)
        for node in self.nodes:
            node.start()

        self.receiving_directory_monitor_thread = threading.Thread(
            name=f"{self.name}-receiving-monitor",
            target=self.monitor_receiving_directory, 
            daemon=True
        )
        self.receiving_directory_monitor_thread.start()

        self.staging_directory_monitor_thread = threading.Thread(
            name=f"{self.name}-staging-monitor",
            target=self.monitor_staging_directory,
            daemon=True
        )
        self.staging_directory_monitor_thread.start()
    
    def join(self):
        for node in self.nodes:
            node.join()

        self.receiving_directory_monitor_thread.join()
        self.staging_directory_monitor_thread.join()

    def stop(self):
        log(f"Stopping graph '{self.name}' with {len(self.nodes)} nodes.", level="info", logger=self.logger)
        self._stop.set()
        for node in self.nodes:
            node.stop_consumers()
