from dataclasses import dataclass
from pathlib import Path
import hashlib
import os

from anacostia.utils.merkle_tree import ProofEntry


@dataclass(frozen=True)
class ChunkInfo:
    index: int
    filename: str
    offset: int
    size: int
    sha256: str


@dataclass(frozen=True)
class ChunkManifest:
    transfer_id: str
    chunk_count: int
    chunk_info: ChunkInfo
    merkle_root: str
    merkle_leaf_index: int
    merkle_proof: list[ProofEntry]


def partition_file(
    source_path: str | Path,
    chunks_directory: str | Path,
    chunk_size: int,
    buffer_size: int = 1024 * 1024,
) -> list[ChunkInfo]:
    """Split a file into fixed-size chunks and hash each chunk."""
    source_path = Path(source_path).resolve()
    chunks_directory = Path(chunks_directory).resolve()

    if not source_path.is_file():
        raise FileNotFoundError(f"Input file does not exist: {source_path}")

    if chunk_size <= 0:
        raise ValueError("chunk_size must be greater than zero")

    if buffer_size <= 0:
        raise ValueError("buffer_size must be greater than zero")

    chunks: list[ChunkInfo] = []
    offset = 0
    index = 0

    with source_path.open("rb") as source:
        while True:
            chunk_filename = f"chunk-{index:06d}.bin"
            chunk_path = chunks_directory / chunk_filename

            bytes_written = 0
            digest = hashlib.sha256()

            with chunk_path.open("xb") as chunk_file:
                while bytes_written < chunk_size:
                    bytes_to_read = min(
                        buffer_size,
                        chunk_size - bytes_written,
                    )

                    block = source.read(bytes_to_read)

                    if not block:
                        break

                    chunk_file.write(block)
                    digest.update(block)
                    bytes_written += len(block)

                if bytes_written:
                    chunk_file.flush()
                    os.fsync(chunk_file.fileno())

            if bytes_written == 0:
                chunk_path.unlink()
                break

            chunks.append(
                ChunkInfo(
                    index=index,
                    filename=chunk_filename,
                    offset=offset,
                    size=bytes_written,
                    sha256=digest.hexdigest(),
                )
            )

            offset += bytes_written
            index += 1

    return chunks
