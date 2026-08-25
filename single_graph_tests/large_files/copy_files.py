import shutil
from pathlib import Path


tests_path = Path("./testing_artifacts")
receiving_directory = tests_path / "receiving_dir"
transport_dir = tests_path / "transport_dir"

for transfer_folder in transport_dir.iterdir():
    parititon_folder = transfer_folder / "partitions"

    for chunk_folder in parititon_folder.iterdir():
        shutil.copytree(chunk_folder, receiving_directory / chunk_folder.name)