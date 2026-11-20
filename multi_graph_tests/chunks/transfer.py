from pathlib import Path
import argparse
import shutil



parser = argparse.ArgumentParser(description="Run the pipeline after restart test")
parser.add_argument("-c", "--copy", action="store_true", help="Flag to indicate whether to copy the transfer packages instead of moving them.")
args = parser.parse_args()

tests_path = Path("./testing_artifacts")
src_path = tests_path / "pipeline1" / "transport_dir"
dest_path = tests_path / "pipeline2" / ".anacostia" / "receiving"

if __name__ == "__main__":
    for transport_package in src_path.iterdir():
        chunks_folder = transport_package / "chunks"

        for chunk in chunks_folder.iterdir():
            if chunk.is_file() and chunk.suffix == ".tar":
                dest_chunk_path = dest_path / chunk.name

                if args.copy:
                    shutil.copy2(chunk, dest_chunk_path)
                    print(f"Copied {chunk} to {dest_chunk_path}")
                else:
                    chunk.rename(dest_chunk_path)
                    print(f"Moved {chunk} to {dest_chunk_path}")
