from pathlib import Path
import json
from typing import Any

from anacostia.producers.base import Producer
from anacostia.utils.hashing import hash_file
from anacostia.utils.types import Artifact, JsonDict



class FileSystemProducer(Producer):
    def register_artifact(self, file_path: Path, metadata: JsonDict = None, content: Any = None):
        artifact_hash = hash_file(file_path)
        artifact_location = json.dumps({"path": str(file_path)})
        artifact = Artifact(hash=artifact_hash, location=artifact_location)
        super().register_artifact(artifact, metadata, content)