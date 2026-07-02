from logging import Logger
import os
from pathlib import Path
from typing import Any, Generator
import time

from anacostia.streams.base import Stream
from anacostia.utils.logging import log
from anacostia.utils.types import JsonDict



class DirectoryStream(Stream):
    def __init__(self, name: str, directory: Path, poll_interval: float = 0.1, logger: Logger = None):
        """
        Initialize a DirectoryStream instance.

        :param name: Name of the stream.
        :param directory: The directory to monitor.
        :param poll_interval: The interval (in seconds) at which the stream polls the directory for new artifacts.
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

    def load_artifact(self, artifact_location: JsonDict) -> bytes:
        # Suppose artifact_location = {"filepath": "/path/to/file.txt"}
        artifact_path = Path(artifact_location["filepath"])

        if artifact_path.is_file():
            with open(artifact_path, "rb") as f:
                return f.read()

        elif artifact_path.is_dir():
            raise IsADirectoryError(f"Expected a file but found a directory: {artifact_path}")

        else:
            raise FileNotFoundError(f"File not found: {artifact_path}")

    def __iter__(self) -> Generator[Any, Any, str]:
        """
        Poll the directory for new artifacts, register the artifacts into the DB, and yield their content and hashes.
        Yields single items: (content, file_hash). User implemented method.
        """

        while True:
            # sort files by last modification time
            for path in sorted(self.directory.iterdir(), key=lambda p: p.stat().st_mtime):

                artifact_location = {"filepath": str(path)}
                if not self.is_artifact_registered(artifact_location):

                    # load, hash, and register artifact
                    artifact_content = self.load_artifact(artifact_location)
                    file_hash = self.hash_artifact(artifact_content)
                    self.register_artifact(file_hash, artifact_location)
                    yield artifact_content, file_hash
                    
            # IMPORTANT: prevent polling from blocking main thread
            time.sleep(self.poll_interval)