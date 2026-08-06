from __future__ import annotations

import gzip
import hashlib
import json
import os
import shutil
import tarfile
import tempfile
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class ChunkInfo:
    index: int
    filename: str
    offset: int
    size: int
    sha256: str


@dataclass(frozen=True)
class ChunkManifest:
    format: str
    version: int
    source_directory: str
    archive_filename: str
    archive_size: int
    archive_sha256: str
    chunk_size: int
    chunk_count: int
    chunks: list[ChunkInfo]


def sha256_file(
    file_path: str | Path,
    *,
    buffer_size: int = 1024 * 1024,
) -> str:
    """Calculate the SHA-256 hash of a file."""
    digest = hashlib.sha256()

    with Path(file_path).open("rb") as file:
        while block := file.read(buffer_size):
            digest.update(block)

    return digest.hexdigest()


def create_deterministic_tar(
    source_directory: str | Path,
    tar_path: str | Path,
) -> Path:
    """
    Create a deterministic tar archive.

    Normalizations:
    - Entries are sorted by relative path.
    - UID and GID are set to zero.
    - User and group names are cleared.
    - Modification times are set to zero.
    - The source folder itself is preserved as the top-level directory.
    """
    source_directory = Path(source_directory).resolve()
    tar_path = Path(tar_path).resolve()

    if not source_directory.is_dir():
        raise NotADirectoryError(
            f"Source directory does not exist: {source_directory}"
        )

    tar_path.parent.mkdir(parents=True, exist_ok=True)

    if tar_path.exists():
        raise FileExistsError(f"Tar file already exists: {tar_path}")

    source_parent = source_directory.parent

    paths = [source_directory]
    paths.extend(
        sorted(
            source_directory.rglob("*"),
            key=lambda path: path.relative_to(source_parent).as_posix(),
        )
    )

    def normalize_tarinfo(tar_info: tarfile.TarInfo) -> tarfile.TarInfo:
        tar_info.uid = 0
        tar_info.gid = 0
        tar_info.uname = ""
        tar_info.gname = ""
        tar_info.mtime = 0

        # Preserve normal rwx permissions only.
        tar_info.mode &= 0o0777

        # Reject files that contain privileged bits.
        if tar_info.mode & (0o4000 | 0o2000):
            raise ValueError("setuid/setgid files are not permitted")

        return tar_info

    with tarfile.open(tar_path, mode="w", format=tarfile.PAX_FORMAT) as archive:
        for path in paths:
            archive_name = path.relative_to(source_parent).as_posix()

            archive.add(
                path,
                arcname=archive_name,
                recursive=False,
                filter=normalize_tarinfo,
            )

    return tar_path


def gzip_file(
    source_path: str | Path,
    gzip_path: str | Path,
    *,
    compression_level: int = 6,
    buffer_size: int = 1024 * 1024,
) -> Path:
    """
    Compress a file using gzip.

    filename="" and mtime=0 prevent the gzip header from embedding the
    original filename or current timestamp.
    """
    source_path = Path(source_path).resolve()
    gzip_path = Path(gzip_path).resolve()

    if not source_path.is_file():
        raise FileNotFoundError(f"Input file does not exist: {source_path}")

    if gzip_path.exists():
        raise FileExistsError(f"Gzip file already exists: {gzip_path}")

    if not 0 <= compression_level <= 9:
        raise ValueError("compression_level must be between 0 and 9")

    gzip_path.parent.mkdir(parents=True, exist_ok=True)

    with source_path.open("rb") as source:
        with gzip_path.open("wb") as raw_output:
            with gzip.GzipFile(
                filename="",
                mode="wb",
                fileobj=raw_output,
                compresslevel=compression_level,
                mtime=0,
            ) as compressed_output:
                shutil.copyfileobj(
                    source,
                    compressed_output,
                    length=buffer_size,
                )

    return gzip_path


def partition_file(
    source_path: str | Path,
    chunks_directory: str | Path,
    *,
    chunk_size: int,
    chunk_prefix: str = "chunk",
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

    chunks_directory.mkdir(parents=True, exist_ok=False)

    chunks: list[ChunkInfo] = []
    offset = 0
    index = 0

    with source_path.open("rb") as source:
        while True:
            chunk_filename = f"{chunk_prefix}-{index:06d}.bin"
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


def package_directory_into_chunks(
    source_directory: str | Path,
    output_directory: str | Path,
    *,
    chunk_size: int = 280 * 1024 * 1024,
    compression_level: int = 6,
    keep_tar_file: bool = False,
) -> ChunkManifest:
    """
    Convert a folder into tar, gzip the tar file, and split it into chunks.

    Output structure:

        output_directory/
        ├── <folder-name>.tar.gz
        ├── chunk_manifest.json
        └── chunks/
            ├── chunk-000000.bin
            ├── chunk-000001.bin
            └── ...

    By default, the intermediate uncompressed .tar file is deleted.
    """
    source_directory = Path(source_directory).resolve()
    output_directory = Path(output_directory).resolve()

    if not source_directory.is_dir():
        raise NotADirectoryError(
            f"Source directory does not exist: {source_directory}"
        )

    if output_directory.exists():
        raise FileExistsError(
            f"Output directory already exists: {output_directory}"
        )

    output_directory.mkdir(parents=True)

    archive_base_name = source_directory.name
    tar_path = output_directory / f"{archive_base_name}.tar"
    gzip_path = output_directory / f"{archive_base_name}.tar.gz"
    chunks_directory = output_directory / "chunks"
    manifest_path = output_directory / "chunk_manifest.json"

    try:
        create_deterministic_tar(
            source_directory=source_directory,
            tar_path=tar_path,
        )

        gzip_file(
            source_path=tar_path,
            gzip_path=gzip_path,
            compression_level=compression_level,
        )

        chunks = partition_file(
            source_path=gzip_path,
            chunks_directory=chunks_directory,
            chunk_size=chunk_size,
        )

        archive_size = gzip_path.stat().st_size
        chunk_total = sum(chunk.size for chunk in chunks)

        if archive_size != chunk_total:
            raise RuntimeError(
                "Chunk sizes do not equal the compressed archive size"
            )

        manifest = ChunkManifest(
            format="anacostia-chunk-manifest",
            version=1,
            source_directory=source_directory.name,
            archive_filename=gzip_path.name,
            archive_size=archive_size,
            archive_sha256=sha256_file(gzip_path),
            chunk_size=chunk_size,
            chunk_count=len(chunks),
            chunks=chunks,
        )

        with manifest_path.open("x", encoding="utf-8") as manifest_file:
            json.dump(
                {
                    **asdict(manifest),
                    "chunks": [asdict(chunk) for chunk in chunks],
                },
                manifest_file,
                indent=2,
            )
            manifest_file.write("\n")

        if not keep_tar_file:
            tar_path.unlink()

        return manifest

    except Exception:
        shutil.rmtree(output_directory, ignore_errors=True)
        raise


if __name__ == "__main__":
    manifest = package_directory_into_chunks(
        source_directory="my_dataset",
        output_directory="transfer_package",
        chunk_size=280 * 1024 * 1024,  # 280 MiB
        compression_level=6,
        keep_tar_file=False,
    )

    print(f"Archive SHA-256: {manifest.archive_sha256}")
    print(f"Archive size:    {manifest.archive_size:,} bytes")
    print(f"Chunk count:     {manifest.chunk_count}")

    for chunk in manifest.chunks:
        print(
            f"{chunk.filename}: "
            f"{chunk.size:,} bytes, "
            f"SHA-256={chunk.sha256}"
        )