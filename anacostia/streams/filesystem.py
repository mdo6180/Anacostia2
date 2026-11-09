from logging import Logger
import os
from pathlib import Path
from collections.abc import Iterator
from typing import Set

from anacostia.streams.base import Stream
from anacostia.utils.logging import log
from anacostia.utils.types import Artifact
from anacostia.utils.hashing import hash_file
from anacostia.utils.package import create_deterministic_tar



class DirectoryStream(Stream):
    def __init__(self, name: str, directory: Path, poll_interval: float = 0.1, hash_chunk_size: int = 1_048_576, logger: Logger = None):
        """
        Initialize a DirectoryStream instance.

        :param name: Name of the stream.
        :param directory: The directory to monitor.
        :param poll_interval: The interval (in seconds) at which the stream polls the directory for new artifacts.
        :param hash_chunk_size: The size of chunks to read when hashing files.
        :param logger: Logger instance for logging.

        Note: there is no hash_chunk_size parameter in this class because 
        the DirectoryStream class assumes files are small enough to be read into memory for hashing. 
        If you need to handle large files, consider implementing a custom stream class that inherits from Stream 
        and overrides the register_artifact method to handle chunked hashing.
        """
        super().__init__(name=name, source=directory, poll_interval=poll_interval, logger=logger)

        self.logger = logger
        if os.path.exists(directory) is False:
            log(f"Directory {directory} does not exist. Creating it.", level="info", logger=self.logger)
            os.makedirs(directory)

        self.name = name
        self.directory: Path = directory
        self.poll_interval = poll_interval
        self.hash_chunk_size = hash_chunk_size

    def __iter__(self) -> Iterator[Artifact]:
        """
        Poll the directory for new artifacts, register them in the database,
        and yield newly discovered artifacts in chronological order.
        """

        while not self.stop_event.is_set():

            # Current files in the directory.
            current_paths: Set = {
                path.resolve() for path in self.directory.iterdir()
            }

            # Previously registered files.
            registered_paths: Set = {
                Path(location["path"]).resolve() 
                for location in self.get_all_artifact_locations()
            }

            # New files only.
            new_paths: Set = current_paths - registered_paths

            # IMPORTANT: Process incoming files in chronological order.
            for path in sorted(new_paths, key=lambda p: p.stat().st_mtime):

                artifact_location = {"path": str(path)}

                if path.is_file():
                    artifact_hash = hash_file(artifact_location["path"])
                
                elif path.is_dir():
                    temp_tar_path = f'{artifact_location["path"]}.tar'
                    tar_path = create_deterministic_tar(artifact_location["path"], temp_tar_path)
                    artifact_hash = hash_file(tar_path)
                    tar_path.unlink()
                
                else:
                    log(f"Skipping {path} as it is neither a file nor a directory.", level="warning", logger=self.logger)
                    continue

                artifact = Artifact(location=artifact_location, hash=artifact_hash)
                self.register_artifact(artifact)        # register the artifact in the stream's local database table
                yield artifact  # yield an Artifact object to the consumer

            # IMPORTANT: prevent polling from blocking the main thread.
            self.stop_event.wait(self.poll_interval)  # Use stop_event to allow graceful exit