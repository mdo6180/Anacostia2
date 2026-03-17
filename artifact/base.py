from abc import ABC, abstractmethod
from typing import Any



class BaseArtifactType(ABC):
    """Base class for artifact types. Users can create their own artifact types by inheriting from this class and implementing the abstract methods."""

    def __init__(self, path: str, hash_chunk_size: int = 1_048_576):
        self.path = path
        self.hash_chunk_size = hash_chunk_size

    @abstractmethod
    def setup(self):
        """
        User implemented method to tell the Stream how to set up the artifact, e.g. create necessary tables in the database, 
        create necessary folders in the filesystem, etc.
        """
        raise NotImplementedError
    
    @abstractmethod
    def register_artifact(self, filepath: str, artifact_hash: str) -> None:
        """
        User implemented method to tell the Stream how to register the artifact, e.g. insert a record into the database.
        Tip: we can save metadata about the artifact in the database, performance metrics, information about artifact origin, etc.
        We can save metadata by saving the data into a json file in the filesystem and then registering the data in the json file into the db.
        """
        raise NotImplementedError

    @abstractmethod
    def hash_artifact(self) -> str:
        """
        User implemented method to tell the Stream how to hash the artifact, e.g. compute the SHA-256 hash of a file.
        """
        raise NotImplementedError

    @abstractmethod
    def load_artifact(self) -> Any:
        """
        User implemented method to tell the Stream how to load the artifact, e.g. read the contents of a file.
        """
        raise NotImplementedError