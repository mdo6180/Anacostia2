from logging import Logger
import os
from pathlib import Path
from typing import Any, Generator
import time
import hashlib

from anacostia.streams.base import Stream
from anacostia.utils.logging import log
from anacostia.utils.types import JsonDict, Artifact



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

    def hash_file(self, artifact_location: JsonDict) -> str:
        """
        Hash the artifact using the specified hash algorithm and return the hash value. User implemented method.
        """
        sha256 = hashlib.sha256()
        with open(artifact_location["filepath"], 'rb') as f:
            while chunk := f.read(self.hash_chunk_size):
                sha256.update(chunk)
        return sha256.hexdigest()

    def __iter__(self) -> Generator[Any, Any, str]:
        """
        Poll the directory for new artifacts, register the artifacts into the DB, and yield their content and hashes.
        Yields single items: (artifact_location, file_hash). User implemented method.
        """

        while True:
            # sort files by last modification time
            for path in sorted(self.directory.iterdir(), key=lambda p: p.stat().st_mtime):

                artifact_location = {"filepath": str(path)}
                if not self.is_artifact_registered(artifact_location):

                    # load, hash, and register artifact
                    if path.is_file():
                        file_hash = self.hash_file(artifact_location)

                    self.register_artifact(file_hash, artifact_location)
                    yield Artifact(location=artifact_location, hash=file_hash)
                    
            # IMPORTANT: prevent polling from blocking main thread
            time.sleep(self.poll_interval)