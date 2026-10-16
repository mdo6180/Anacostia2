from __future__ import annotations

import os
import shutil
import tarfile
import tempfile
from dataclasses import dataclass
from pathlib import Path, PurePosixPath


class UnsafeArchiveError(Exception):
    """Raised when an archive violates the extraction policy."""


@dataclass(frozen=True)
class ArchiveEntry:
    path: str
    entry_type: str
    size: int


def inspect_and_extract_tar_gz(
    archive_path: str | Path,
    destination: str | Path,
    *,
    max_entries: int = 100_000,
    max_total_size: int = 20 * 1024**3,  # 20 GiB
    max_file_size: int = 5 * 1024**3,    # 5 GiB
    copy_buffer_size: int = 1024**2,     # 1 MiB
) -> list[ArchiveEntry]:
    """
    Validate and safely extract a .tar.gz archive.

    Security policy:
    - Only regular files and directories are allowed.
    - Absolute paths are rejected.
    - '..' traversal is rejected.
    - Backslashes and NUL bytes in names are rejected.
    - Symbolic links and hard links are rejected.
    - Device files, FIFOs, and other special entries are rejected.
    - Duplicate paths are rejected.
    - Existing destinations are never overwritten.
    - File-count and expanded-size limits are enforced.
    - Archive permissions, ownership, and timestamps are not restored.

    The archive is first validated in full. Extraction then occurs inside
    a temporary staging directory. The staging directory is renamed to the
    final destination only after every file has been written successfully.

    Returns:
        Metadata describing the validated archive entries.

    Raises:
        FileNotFoundError:
            If the archive does not exist.

        FileExistsError:
            If the destination already exists.

        UnsafeArchiveError:
            If the archive violates the security policy.

        tarfile.TarError:
            If the input is not a valid gzip-compressed tar archive.
    """
    archive_path = Path(archive_path)
    destination = Path(destination)

    if not archive_path.is_file():
        raise FileNotFoundError(f"Archive not found: {archive_path}")

    if destination.exists():
        raise FileExistsError(
            f"Destination already exists; refusing to overwrite it: "
            f"{destination}"
        )

    if max_entries <= 0:
        raise ValueError("max_entries must be greater than zero")

    if max_total_size <= 0:
        raise ValueError("max_total_size must be greater than zero")

    if max_file_size <= 0:
        raise ValueError("max_file_size must be greater than zero")

    if copy_buffer_size <= 0:
        raise ValueError("copy_buffer_size must be greater than zero")

    destination_parent = destination.parent.resolve()
    destination_parent.mkdir(parents=True, exist_ok=True)

    inspected_entries: list[ArchiveEntry] = []
    validated_paths: dict[PurePosixPath, str] = {}
    casefolded_paths: set[str] = set()

    total_file_size = 0
    entry_count = 0

    # ------------------------------------------------------------------
    # Phase 1: inspect and validate the complete archive.
    # ------------------------------------------------------------------
    with tarfile.open(archive_path, mode="r:gz") as archive:
        for member in archive:
            entry_count += 1

            if entry_count > max_entries:
                raise UnsafeArchiveError(
                    f"Archive contains more than {max_entries:,} entries"
                )

            normalized_path = _validate_member_path(member.name)

            # Prevent multiple archive entries from referring to the same
            # exact path.
            if normalized_path in validated_paths:
                raise UnsafeArchiveError(
                    f"Duplicate archive path: {member.name!r}"
                )

            # This stricter check prevents collisions such as README and
            # readme when an archive is moved between case-sensitive and
            # case-insensitive filesystems.
            casefolded = normalized_path.as_posix().casefold()

            if casefolded in casefolded_paths:
                raise UnsafeArchiveError(
                    f"Case-insensitive path collision: {member.name!r}"
                )

            if member.isdir():
                entry_type = "directory"
                entry_size = 0

            elif member.isfile():
                entry_type = "file"
                entry_size = member.size

                if member.size < 0:
                    raise UnsafeArchiveError(
                        f"Negative file size for {member.name!r}"
                    )

                if member.size > max_file_size:
                    raise UnsafeArchiveError(
                        f"File exceeds the per-file size limit: "
                        f"{member.name!r} is {member.size:,} bytes"
                    )

                total_file_size += member.size

                if total_file_size > max_total_size:
                    raise UnsafeArchiveError(
                        f"Expanded archive exceeds the total-size limit of "
                        f"{max_total_size:,} bytes"
                    )

            elif member.issym():
                raise UnsafeArchiveError(
                    f"Symbolic links are not permitted: "
                    f"{member.name!r} -> {member.linkname!r}"
                )

            elif member.islnk():
                raise UnsafeArchiveError(
                    f"Hard links are not permitted: "
                    f"{member.name!r} -> {member.linkname!r}"
                )

            elif member.ischr():
                raise UnsafeArchiveError(
                    f"Character devices are not permitted: {member.name!r}"
                )

            elif member.isblk():
                raise UnsafeArchiveError(
                    f"Block devices are not permitted: {member.name!r}"
                )

            elif member.isfifo():
                raise UnsafeArchiveError(
                    f"FIFOs are not permitted: {member.name!r}"
                )

            else:
                raise UnsafeArchiveError(
                    f"Unsupported tar entry type for {member.name!r}: "
                    f"{member.type!r}"
                )

            validated_paths[normalized_path] = entry_type
            casefolded_paths.add(casefolded)

            inspected_entries.append(
                ArchiveEntry(
                    path=normalized_path.as_posix(),
                    entry_type=entry_type,
                    size=entry_size,
                )
            )

    _validate_path_relationships(validated_paths)

    # ------------------------------------------------------------------
    # Phase 2: extract approved entries into an isolated staging folder.
    # ------------------------------------------------------------------
    staging_root = Path(
        tempfile.mkdtemp(
            prefix=f".{destination.name}.extracting-",
            dir=destination_parent,
        )
    )

    try:
        with tarfile.open(archive_path, mode="r:gz") as archive:
            for member in archive:
                relative_path = _validate_member_path(member.name)
                target = staging_root.joinpath(*relative_path.parts)

                _ensure_path_is_within_directory(target, staging_root)

                if member.isdir():
                    target.mkdir(
                        mode=0o755,
                        parents=True,
                        exist_ok=True,
                    )
                    continue

                if not member.isfile():
                    # This should already have been rejected during the first
                    # pass. Keep the extraction pass defensive anyway.
                    raise UnsafeArchiveError(
                        f"Entry changed type between validation and extraction: "
                        f"{member.name!r}"
                    )

                target.parent.mkdir(
                    mode=0o755,
                    parents=True,
                    exist_ok=True,
                )

                # "xb" guarantees that extraction never overwrites an entry
                # that already exists in the staging directory.
                source = archive.extractfile(member)

                if source is None:
                    raise UnsafeArchiveError(
                        f"Could not open archived file: {member.name!r}"
                    )

                bytes_written = 0

                with source, target.open("xb") as output:
                    while True:
                        block = source.read(copy_buffer_size)

                        if not block:
                            break

                        output.write(block)
                        bytes_written += len(block)

                    output.flush()
                    os.fsync(output.fileno())

                if bytes_written != member.size:
                    raise UnsafeArchiveError(
                        f"Extracted-size mismatch for {member.name!r}: "
                        f"expected {member.size:,} bytes, "
                        f"wrote {bytes_written:,} bytes"
                    )

                # Do not reproduce executable, setuid, or other archived
                # permission bits.
                target.chmod(0o644)

        # The final destination still must not exist. os.replace() could
        # overwrite some destination types, so explicitly check again.
        if destination.exists():
            raise FileExistsError(
                f"Destination appeared during extraction: {destination}"
            )

        staging_root.rename(destination)

    except Exception:
        shutil.rmtree(staging_root, ignore_errors=True)
        raise

    return inspected_entries


def _validate_member_path(member_name: str) -> PurePosixPath:
    """Validate and normalize one tar member path."""
    if not member_name:
        raise UnsafeArchiveError("Archive contains an empty path")

    if "\x00" in member_name:
        raise UnsafeArchiveError(
            f"Archive path contains a NUL byte: {member_name!r}"
        )

    # Tar paths are POSIX paths. Rejecting backslashes avoids ambiguous
    # behavior if the extracted directory is later processed on Windows.
    if "\\" in member_name:
        raise UnsafeArchiveError(
            f"Backslashes are not permitted in archive paths: "
            f"{member_name!r}"
        )

    path = PurePosixPath(member_name)

    if path.is_absolute():
        raise UnsafeArchiveError(
            f"Absolute archive path is not permitted: {member_name!r}"
        )

    # Ignore harmless leading "./" components while retaining a canonical
    # representation for duplicate detection.
    components = tuple(
        component
        for component in path.parts
        if component not in ("", ".")
    )

    if not components:
        raise UnsafeArchiveError(
            f"Archive path does not identify a file or directory: "
            f"{member_name!r}"
        )

    if ".." in components:
        raise UnsafeArchiveError(
            f"Parent traversal is not permitted: {member_name!r}"
        )

    # Reject names that can be interpreted as Windows drive paths.
    if ":" in components[0]:
        raise UnsafeArchiveError(
            f"Drive-like archive path is not permitted: {member_name!r}"
        )

    return PurePosixPath(*components)


def _validate_path_relationships(
    validated_paths: dict[PurePosixPath, str],
) -> None:
    """
    Reject archives that place descendants beneath regular files.

    Example of an invalid archive:

        models             # regular file
        models/model.bin   # child beneath that regular file
    """
    for path in validated_paths:
        parents = path.parents

        for parent in parents:
            if parent == PurePosixPath("."):
                continue

            parent_type = validated_paths.get(parent)

            if parent_type == "file":
                raise UnsafeArchiveError(
                    f"Archive path {path.as_posix()!r} is located beneath "
                    f"regular file {parent.as_posix()!r}"
                )


def _ensure_path_is_within_directory(
    candidate: Path,
    directory: Path,
) -> None:
    """Ensure a filesystem path resolves beneath the expected directory."""
    directory_resolved = directory.resolve()
    candidate_resolved = candidate.resolve(strict=False)

    try:
        candidate_resolved.relative_to(directory_resolved)
    except ValueError as exc:
        raise UnsafeArchiveError(
            f"Extraction path escapes the destination: {candidate}"
        ) from exc