import hashlib
from logging import Logger
import os
import time
from typing import Any, Generator



class DirectoryStream:
    def __init__(self, directory: str, poll_interval: float = 0.1, hash_chunk_size: int = 1_048_576, logger: Logger = None):
        self.logger = logger
        if os.path.exists(directory) is False:
            self.logger.info(f"Directory {directory} does not exist. Creating it.")
            os.makedirs(directory)

        self.directory = directory
        self.poll_interval = poll_interval
        self.hash_chunk_size = hash_chunk_size
        self.seen = set()
        self.connection = None

    def set_db_connection(self, connection):
        self.connection = connection
        # add logic to create necessary tables if needed
        # create a stream_artifacts table to track seen artifacts for all streams
        # add a way to get indexes from seen artifacts to help with resuming streams after restart
        # associate the file paths with file hashes in the DB for this stream
    
    def hash_file(self, filepath: str) -> str:
        sha256 = hashlib.sha256()
        with open(filepath, 'rb') as f:
            while chunk := f.read(self.hash_chunk_size):
                sha256.update(chunk)
        return sha256.hexdigest()

    def __iter__(self) -> Generator[Any, Any, str]:
        """
        Yields single items: (content, file_hash)
        """
        while True:
            for filename in sorted(os.listdir(self.directory)):

                if filename in self.seen:   # replace with artifact_exists DB check in the future
                    continue

                path = os.path.join(self.directory, filename)
                if not os.path.isfile(path):
                    continue

                self.seen.add(filename)     # replace with register_artifact DB call in the future

                file_hash = self.hash_file(path)

                # Read file content, user will implement their own logic to extract the content from the artifact
                with open(path, "r") as file:
                    content = file.read()
                    yield content, file_hash

            # IMPORTANT: put this sleep here so the polling doesn't block the main thread.
            time.sleep(self.poll_interval)