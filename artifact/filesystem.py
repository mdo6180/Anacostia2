import hashlib
from pathlib import Path

from artifact.base import BaseArtifact



class FileArtifact(BaseArtifact):
    def hash_artifact(self) -> str:
        sha256 = hashlib.sha256()
        with open(self.path, "rb") as f:
            while chunk := f.read(self.hash_chunk_size):
                sha256.update(chunk)
        return sha256.hexdigest()

    def load_artifact(self) -> str:
        with open(self.path, "r") as f:
            return f.read()