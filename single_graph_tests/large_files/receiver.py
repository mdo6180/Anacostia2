from pathlib import Path
import threading
import json
import argparse
import shutil
import time

from merkle_tree import verify_proof, ProofEntry
from package import hash_file
from anacostia.utils.debug import attach_debugger



tests_path = Path("./testing_artifacts")
receiving_directory = tests_path / "receiving_dir"
storage_directory = tests_path / "storage_dir"

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

if args.restart == False:
    if receiving_directory.exists() is True:
        shutil.rmtree(receiving_directory)
    receiving_directory.mkdir(parents=True, exist_ok=True)

    if storage_directory.exists() is True:
        shutil.rmtree(storage_directory)
    storage_directory.mkdir(parents=True, exist_ok=True)



class Receiver:
    def __init__(self, receiving_directory: Path, storage_directory: Path):
        self.receiving_directory = receiving_directory
        self.receiving_directory.mkdir(parents=True, exist_ok=True)
        
        self.storage_directory = storage_directory
        self.storage_directory.mkdir(parents=True, exist_ok=True)

        self._stop = threading.Event()

    def start(self):
        def _receive_chunks():
            while self._stop.is_set() is False:

                # Assumption: everything in the receiving directory is a chunk folder
                for chunk_folder in self.receiving_directory.iterdir():
                    if chunk_folder.is_dir():
                        transfer_manifest = chunk_folder / "transfer_manifest.json"
                        chunk_manifest = chunk_folder / "chunk_manifest.json"

                        try:
                            with (
                                open(transfer_manifest, "r") as transfer_manifest_file,
                                open(chunk_manifest, "r") as chunk_manifest_file
                            ):
                                transfer_manifest_data = json.load(transfer_manifest_file)
                                transfer_id = transfer_manifest_data["transfer_id"]

                                chunk_manifest_data = json.load(chunk_manifest_file)

                                # Expected SHA-256 hash
                                chunk_sha256 = chunk_manifest_data["chunk_info"]["sha256"]

                                # Check 1: is the chunk the same as what the manifest says it is?
                                # Hash chunk and check to see if actual chunk hash == hash provided in manifest
                                chunk_path = chunk_folder / chunk_manifest_data["chunk_info"]["filename"]
                                actual_sha256 = hash_file(chunk_path)
                                if actual_sha256 != chunk_sha256:
                                    print(f"Actual chunk hash is not the same as expected chunk hash in manifest")
                                    continue

                                # Check 2: does chunk belong to the same artifact?
                                # Check if the hash satisfies the merkle tree
                                merkle_proof = chunk_manifest_data["merkle_proof"]
                                merkle_proof = [
                                    ProofEntry(
                                        side=entry['side'], 
                                        hash=bytes.fromhex(entry['hash'])
                                    ) for entry in merkle_proof
                                ]

                                merkle_root = chunk_manifest_data["merkle_root"]
                                merkle_root = bytes.fromhex(merkle_root)

                                verified = verify_proof(
                                    item=bytes.fromhex(chunk_sha256), 
                                    proof=merkle_proof, 
                                    expected_root=merkle_root
                                )
                                if verified is False:
                                    print(f"Merkle proof verification failed for transfer_id: {transfer_id}")
                                    continue

                                transfer_dir = self.storage_directory / transfer_id
                                if transfer_dir.exists() is False:
                                    transfer_dir.mkdir(parents=True, exist_ok=True)

                                # check if folder for transfer_id already exists in storage_directory
                                # if it does not exist, move the chunk folder to the storage directory
                                destination_folder = transfer_dir / chunk_folder.name
                                if destination_folder.exists() is False:
                                    chunk_folder.rename(destination_folder)

                        except FileNotFoundError:
                            # Sometimes the chunk binary is so big that it takes the OS some time to copy over 
                            # both the binary and the transfer manifest chunk folder.
                            # Because the chunk takes some time to copy over, the open() command will fail and throw a FileNotFoundError
                            # because the transfer manifest has not been transfered yet.

                            # if the chunk binary has been copied successfully but the transfer manifest still hasn't arrived,
                            # then we need to throw a warning and move onto other packages.
                            # Eventually we will come back to check on this chunk to see if maybe the user has found the transfer manifest.
                            print("Warning: No transfer manifest detected")

                        """
                        if chunk_binary.exists() and chunk_manifest.exists():
                            # Move the chunk folder to the storage directory
                            destination_folder = self.storage_directory / chunk_folder.name
                            folder.rename(destination_folder)
                        """

                time.sleep(0.1)

        self.thread = threading.Thread(target=_receive_chunks, daemon=True)
        self.thread.start()

    def stop(self):
        self._stop.set()


    
def combine_chunks(
    chunks_directory: Path,
    chunk_prefix: str,
    output_path: Path,
    buffer_size: int = 1024 * 1024,
) -> None:
    if buffer_size <= 0:
        raise ValueError("buffer_size must be greater than zero")

    output_path.parent.mkdir(parents=True, exist_ok=True)

    for chunk_folder in sorted(chunks_directory.iterdir()):
        chunk_binary = chunk_folder / f"{chunk_prefix}.bin"
        transfer_manifest = chunk_folder / "transfer_manifest.json"

    """
    with output_path.open("wb") as output_file:
        index = 0

        while True:
            chunk_filename = f"{chunk_prefix}-{index:06d}.bin"
            chunk_path = chunks_directory / chunk_filename

            if not chunk_path.exists():
                break

            with chunk_path.open("rb") as chunk_file:
                while True:
                    block = chunk_file.read(buffer_size)

                    if not block:
                        break

                    output_file.write(block)

            index += 1
    """


if __name__ == "__main__":
    #chunk_directory = Path("./testing_artifacts/transport_dir")

    receiver = Receiver(receiving_directory=receiving_directory, storage_directory=storage_directory)
    receiver.start()

    try:
        receiver.thread.join()
    except KeyboardInterrupt:
        print("Receiver interrupted. Exiting...")
        receiver.stop()
