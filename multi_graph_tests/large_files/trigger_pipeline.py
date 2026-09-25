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
    tests_path = Path("./testing_artifacts") / "pipeline1"
    input_path1 = tests_path / "incoming1"

    for i in range(12):
        create_file(input_path1 / f"test_file{i}.txt", f"incoming1 {i}")
        time.sleep(1.5)