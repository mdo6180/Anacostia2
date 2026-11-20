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



def recombine_file(
    chunks_directory: str | Path,
    output_path: str | Path,
    manifests: list[ChunkManifest],
    buffer_size: int = 1024 * 1024,
) -> Path:
    """Validate and recombine chunks belonging to a single transfer."""
    chunks_directory = Path(chunks_directory).resolve()
    output_path = Path(output_path).resolve()

    if not manifests:
        raise ValueError("No chunk manifests provided")

    if buffer_size <= 0:
        raise ValueError("buffer_size must be greater than zero")

    manifests = sorted(
        manifests,
        key=lambda manifest: manifest.chunk_info.index,
    )

    reference = manifests[0]

    if len(manifests) != reference.chunk_count:
        raise ValueError("Incomplete transfer")

    offset = 0

    # Validate manifest consistency.
    for expected_index, manifest in enumerate(manifests):
        chunk = manifest.chunk_info

        if manifest.transfer_id != reference.transfer_id:
            raise ValueError("Transfer ID mismatch")

        if manifest.chunk_count != reference.chunk_count:
            raise ValueError("Chunk count mismatch")

        if manifest.merkle_root != reference.merkle_root:
            raise ValueError("Merkle root mismatch")

        if chunk.index != expected_index:
            raise ValueError("Missing or duplicate chunk index")

        if chunk.offset != offset:
            raise ValueError("Invalid chunk offset")

        if manifest.merkle_leaf_index != chunk.index:
            raise ValueError("Merkle leaf index mismatch")

        if chunk.size <= 0:
            raise ValueError("Invalid chunk size")

        if chunk.filename != f"chunk-{chunk.index:06d}.bin":
            raise ValueError("Invalid chunk filename")

        offset += chunk.size

    # Reconstruct the original file.
    output = output_path.open("xb")

    try:
        with output:
            for manifest in manifests:
                chunk = manifest.chunk_info
                chunk_path = chunks_directory / chunk.filename

                digest = hashlib.sha256()
                bytes_read = 0

                with chunk_path.open("rb") as chunk_file:
                    while block := chunk_file.read(buffer_size):
                        output.write(block)
                        digest.update(block)
                        bytes_read += len(block)

                if bytes_read != chunk.size:
                    raise ValueError(
                        f"Size mismatch: {chunk.filename}"
                    )

                if digest.hexdigest() != chunk.sha256:
                    raise ValueError(
                        f"Hash mismatch: {chunk.filename}"
                    )

            output.flush()
            os.fsync(output.fileno())

    except BaseException:
        output_path.unlink(missing_ok=True)
        raise

    return output_path
