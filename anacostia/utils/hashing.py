import hashlib
from typing import IO
from pathlib import Path


def hash_file_object(file_obj: IO[bytes]) -> str:
    """Reads bytes from a file-like object in chunks and returns its hex hash.

    Args:
        file_obj: An open binary file-like object (e.g., from open(),
          tarfile.extractfile()).

    Returns:
        The hex-encoded string of the calculated hash.
    """
    # Initialize the hasher based on the selected algorithm string
    digest = hashlib.sha256()

    # Read the file in 64KB chunks to efficiently handle large files
    for chunk in iter(lambda: file_obj.read(65536), b""):
        digest.update(chunk)

    return digest.hexdigest()


def hash_string(input_string: str) -> str:
    """Returns the SHA-256 hash of the input string.

    Args:
        input_string: The string to be hashed.
    """
    return hashlib.sha256(input_string.encode()).hexdigest()


def hash_file(chunk_path: Path, buffer_size: int = 1024 * 1024) -> str:
    sha256 = hashlib.sha256()
    with open(chunk_path, 'rb') as f:
        while chunk := f.read(buffer_size):
            sha256.update(chunk)
    return sha256.hexdigest()
