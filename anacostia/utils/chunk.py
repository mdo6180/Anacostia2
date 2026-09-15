from dataclasses import dataclass
from anacostia.utils.merkle_tree import ProofEntry


@dataclass(frozen=True)
class ChunkInfo:
    index: int
    filename: str
    offset: int
    size: int
    sha256: str

@dataclass(frozen=True)
class TransferArtifact:
    artifact_hash: str
    filepath: str
    size_bytes: int
    source_pipeline_name: str
    destination_pipeline_name: str
    destination_stream: str = None  # Optional field for the destination stream name (database will not have this field)

@dataclass(frozen=True)
class TransferManifest:
    transfer_id: str
    transfer_artifacts: list[TransferArtifact]
    archive_size: int
    archive_sha256: str
    chunk_count: int
    previous_transfer_id: str = None  # Optional field for the previous transfer ID
    previous_transfer_sha256: str = None  # Optional field for the previous transfer SHA256


@dataclass(frozen=True)
class ChunkManifest:
    transfer_id: str
    chunk_count: int
    chunk_info: ChunkInfo
    merkle_root: str
    merkle_leaf_index: int
    merkle_proof: list[ProofEntry]


@dataclass(frozen=True)
class TransferManifestSignature:
    transfer_id: str
    manifest_signature: str
    manifest_hash: str
