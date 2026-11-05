import time
from pathlib import Path


def create_file(file_path, content):
    try:
        with open(file_path, 'w') as file:
            file.write(content)
        print(f"File '{file_path}' created successfully.")
    except Exception as e:
        print(f"Error creating the file: {e}")


if __name__ == "__main__":
    testing_artifacts_dir = Path("./testing_artifacts")
    pipeline2_dir = testing_artifacts_dir / "pipeline2"
    input_path2 = pipeline2_dir / "high_side_local"

    for i in range(1):
        num_artifacts = len([f for f in input_path2.iterdir() if f.is_file()])
        create_file(input_path2 / f"test_file{num_artifacts}.txt", f"incoming2 {i}")
        time.sleep(1.5)