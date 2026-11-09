from __future__ import annotations

import gzip
import shutil
import tarfile
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class TransferArtifact:
    artifact_hash: str
    filepath: str
    size_bytes: int
    destination_stream: str = None  # Optional field for the destination stream name (database will not have this field)

@dataclass(frozen=True)
class TransferManifest:
    transfer_id: str
    transfer_artifacts: list[TransferArtifact]
    archive_size: int
    archive_sha256: str
    chunk_count: int
    source_pipeline_name: str
    destination_pipeline_name: str
    previous_transfer_id: str = None  # Optional field for the previous transfer ID
    previous_transfer_sha256: str = None  # Optional field for the previous transfer SHA256

@dataclass(frozen=True)
class TransferManifestSignature:
    transfer_id: str
    manifest_signature: str
    manifest_hash: str


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

