import argparse
import logging
import os
from logging import Logger
import shutil
from pathlib import Path
from contextlib import contextmanager
from uuid import uuid4
from dataclasses import asdict, dataclass
import json

from anacostia.utils.debug import attach_debugger
from anacostia.utils.types import Artifact

from package import create_deterministic_tar, gzip_file, partition_file, sha256_file
from merkle_tree import ProofEntry, generate_proof, merkle_root



tests_path = Path("./testing_artifacts")
db_folder_path = tests_path / ".anacostia"
input_path1 = tests_path / "incoming1"
input_path2 = tests_path / "incoming2"
output_path1 = tests_path / "processed1"
output_path2 = tests_path / "processed2"
output_combined_path = tests_path / "processed_combined"
transport_package_dir = tests_path / "transport_dir"
pipeline2_receiver = tests_path / "transport_receiver"

parser = argparse.ArgumentParser(description="Run the pipeline after restart test")
parser.add_argument("-r", "--restart", action="store_true", help="Flag to indicate if this is a restart")
parser.add_argument("-d", "--debug", action="store_true", help="Flag to indicate if debugging is enabled")
args = parser.parse_args()

if args.debug:
    # To debug this test:
    # Add a breakpoint by clicking on the left side of the line number you want to break on.
    # run the script: python mid_stream_stop.py -r -d
    # open the debug tab in vscode
    # select the "Python Debugger: Remote Attach" configuration, then click on the play button.
    # The script will pause at the breakpoint and you can inspect the values of variables in the debug console.
    attach_debugger()

def create_text_file(path: str, size_mb: int = 10):
    target_size = size_mb * 1024 * 1024  # bytes
    line = "The quick brown fox jumps over the lazy dog.\n"

    with open(path, "w", encoding="utf-8") as f:
        while f.tell() < target_size:
            f.write(line)

        # Trim to exactly the target size
        f.truncate(target_size)

if args.restart == False:
    if tests_path.exists() is True:
        shutil.rmtree(tests_path)
    tests_path.mkdir(parents=True, exist_ok=True)
    create_text_file(str(tests_path / "10mb.txt"), size_mb=10)

log_path = tests_path / "anacostia.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=str(log_path),
    filemode='a'
)
logger = logging.getLogger(__name__)


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
    chunk_info: ChunkInfo
    merkle_root: str
    merkle_leaf_index: int
    merkle_proof: list[ProofEntry]
    previous_transfer_id: str = None  # Optional field for the previous transfer ID
    previous_transfer_sha256: str = None  # Optional field for the previous transfer SHA256


class FileSystemTransport:
    def __init__(
        self, name: str, 
        packages_directory: Path, 
        hash_chunk_size: int = 1_048_576, 
        logger: Logger = None
    ):
        """
        name: name of the Transport
        packages_directory: directory where all of the Transport's packages will be stored.
        hash_chunk_size: size of the chunks to read when hashing files.
        partition_size: size of the partitions when partitioning files.
        logger: logger for logging statements
        """

        self.name = name
        self.hash_chunk_size = hash_chunk_size
        self.logger = logger
        self.run_id = 0
        self.local_table_name = f"{self.name}_local"
        self.global_usage_table_name = "artifact_usage_events"

        self.packages_directory = packages_directory
        if not self.packages_directory.exists():
            os.makedirs(self.packages_directory)

        self.transfer_artifacts = []  # List to keep track of artifacts added to the transfer package
        self.pipeline_name = None  # Initialize pipeline_name to None

    def set_db_folder(self, db_folder: str):
        self.db_folder = db_folder

    def set_pipeline_name(self, pipeline_name: str):
        self.pipeline_name = pipeline_name

    def register_artifact(self, artifact: Artifact):
        # Logic to add the artifact's hash to the provenance graph
        print(f"Registering artifact {artifact.location} with hash {artifact.hash} in the provenance graph.")
        
    @contextmanager
    def create_transfer_package(self, compression_level: int = 6, partition_size: int = 1_048_576):
        """
        Context manager to create a package for the given artifact.
        Yields the path to the /data folder where all the files for the transfer package should be placed.
        Copy the artifact file into this folder, and when the context is exited, the package will be finalized (e.g., zipped, hashed, and partitioned).
        """

        # Logic to create a folder for the transfer package inside the destination directory,
        # create the /data folder, and yield the path to it.
        package_path = self.packages_directory / f"transfer_{uuid4().hex}"
        self.data_folder_path = package_path / "data"
        self.data_folder_path.mkdir(parents=True, exist_ok=True)

        try:
            yield self.data_folder_path    # Yield the path to the /data folder and self for further operations

            # TODO: copy the database file into the /data folder
            with open(self.data_folder_path / "db_file.txt", "w") as db_file:
                db_file.write("This is a placeholder for the database file.")

            print(f"Contents of the /data folder before packaging: {[f.name for f in self.data_folder_path.iterdir()]}")
            print(f"Size of the /data folder before packaging: {sum(f.stat().st_size for f in self.data_folder_path.iterdir())} bytes")

            # convert the /data folder to a .tar file after the context is exited (i.e., after the user has copied the artifact file into it)
            tar_path = self.data_folder_path.parent / f"{self.data_folder_path.name}.tar"
            tar_path = create_deterministic_tar(self.data_folder_path, tar_path)
            print(f"tar file size: {tar_path.stat().st_size} bytes")

            shutil.rmtree(self.data_folder_path)  # Remove the /data folder after creating the .tar file

            # compress the .tar file into a .tar.gz file
            gzip_path = tar_path.with_suffix(tar_path.suffix + ".gz")
            gzip_path = gzip_file(tar_path, gzip_path, compression_level=compression_level)
            gzip_sha256 = sha256_file(gzip_path)
            print(f"gzip file size: {gzip_path.stat().st_size} bytes")

            os.remove(tar_path)  # Remove the .tar file after creating the .tar.gz file

            partitioned_dir = gzip_path.parent / "partitions"
            chunks = partition_file(gzip_path, partitioned_dir, chunk_size=partition_size)
            print(f"divided gzip file into {len(list(partitioned_dir.iterdir()))} partitions with sizes (bytes): {[f.stat().st_size for f in partitioned_dir.iterdir()]}")

            for chunk in chunks:
                chunk_folder = partitioned_dir / f"{package_path.name}_chunk_{chunk.index}"
                chunk_folder.mkdir(parents=True, exist_ok=True)
                chunk_file_path = chunk_folder / chunk.filename
                shutil.move(str(partitioned_dir / chunk.filename), str(chunk_file_path))

                hashes = [bytes.fromhex(chunk.sha256) for chunk in chunks]
                proof = generate_proof(hashes, leaf_index=chunk.index)
                proof = [ProofEntry(side=entry.side, hash=entry.hash.hex()) for entry in proof]
                root = merkle_root(hashes).hex()

                transfer_manifest = TransferManifest(
                    transfer_id=package_path.name,
                    transfer_artifacts=self.transfer_artifacts,
                    archive_size=gzip_path.stat().st_size,
                    archive_sha256=gzip_sha256,
                    chunk_count=len(chunks),
                    chunk_info=chunk,
                    merkle_root=root,
                    merkle_leaf_index=chunk.index,      # Note: leaf index is the index of the chunk in the list of chunks
                    merkle_proof=proof,
                    previous_transfer_id=None,  # Set to None for the first transfer
                    previous_transfer_sha256=None  # Set to None for the first transfer, otherwise it would be the hash of the previous tranfer manifest file
                )
                transfer_manifest_path = chunk_folder / "transfer_manifest.json"
                with transfer_manifest_path.open("x", encoding="utf-8") as manifest_file:
                    json.dump(
                        {
                            **asdict(transfer_manifest),
                            "transfer_artifacts": [asdict(artifact) for artifact in self.transfer_artifacts],
                        },
                        manifest_file,
                        indent=2,
                    )
                    manifest_file.write("\n")

            os.remove(gzip_path)  # Remove the .tar.gz file after partitioning

        finally:
            # Clean up if necessary
            pass
    
    def add_to_package(self, artifact_hash: str, src_path: Path, dest_path: Path, dest_pipeline_name: str, dest_stream: str) -> Artifact:
        """
        Add an artifact to the transfer package by moving it from the staging directory to the final directory,
        hashing it, and registering it in the local and global databases.

        Args:
            artifact_hash (str): The hash of the artifact to be added to the package.
            src_path (Path): The path to the artifact in the staging directory.
            dest_path (Path): The path to move the artifact to in the package's /data directory. 
            dest_stream (str): The name of the destination stream.
            Note: The final path must be within the directory specified in the directory argument in the class constructor.
            Note: The final path must be within the directory specified in the directory argument in the class constructor.

        Returns:
            Artifact: The committed artifact object where Artifact(location={"path": str(dest_path)}, hash=artifact_hash).
        """

        if not isinstance(src_path, Path):
            raise TypeError("src_path must be of type pathlib.Path")

        if not isinstance(dest_path, Path): 
            raise TypeError("dest_path must be of type pathlib.Path")
        
        if not dest_path.is_relative_to(self.packages_directory):
            raise ValueError(f"Destination path {dest_path} is not within the directory {self.packages_directory}")

        # move the artifact
        dest_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(src_path), str(dest_path))

        filepath = dest_path.relative_to(self.data_folder_path).as_posix()  # Store the relative path to the packages directory

        self.transfer_artifacts.append(TransferArtifact(
            artifact_hash=artifact_hash,
            filepath=filepath,
            size_bytes=dest_path.stat().st_size,
            source_pipeline_name=self.pipeline_name,
            destination_pipeline_name=dest_pipeline_name,
            destination_stream=dest_stream
        ))

    

if __name__ == "__main__":
    artifact_path = tests_path / "10mb.txt"

    artifact = Artifact(
        location={"path": str(artifact_path)},
        hash=sha256_file(artifact_path)
    )

    # Example usage of the FileSystemTransport
    transport = FileSystemTransport(
        name="example_transport", packages_directory=transport_package_dir, logger=logger
    )
    transport.set_pipeline_name("example_src_pipeline")
    with transport.create_transfer_package(partition_size=10000) as data_folder_path:   # 10 KB partitions
        artifact_path = Path(artifact.location["path"])
        #shutil.copy(artifact_path, data_folder_path / artifact_path.name)
        transport.add_to_package(
            artifact_hash=artifact.hash,
            src_path=Path(artifact.location["path"]),
            dest_path=data_folder_path / artifact_path.name,
            dest_pipeline_name="example_dest_pipeline",
            dest_stream="example_dest_stream"
        )