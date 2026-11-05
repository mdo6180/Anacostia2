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
        if transport_package.is_file() and transport_package.suffix == ".tar":
            dest_file_path = dest_path / transport_package.name

            if args.copy:
                shutil.copy2(transport_package, dest_file_path)
                print(f"Copied {transport_package} to {dest_file_path}")
            else:
                transport_package.rename(dest_file_path)
                print(f"Moved {transport_package} to {dest_file_path}")