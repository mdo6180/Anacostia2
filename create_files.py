import time


def create_file(file_path, content):
    try:
        with open(file_path, 'w') as file:
            file.write(content)
        print(f"File '{file_path}' created successfully.")
    except Exception as e:
        print(f"Error creating the file: {e}")


if __name__ == "__main__":
    path = f"./testing_artifacts"
    data_store1 = f"{path}/data_store1"
    data_store2 = f"{path}/data_store2"

    for i in range(10):
        create_file(f"{data_store1}/test_file{i}.txt", f"test file {i}")
        time.sleep(1.5)
        create_file(f"{data_store2}/test_file{i}.txt", f"test file {i}")
        time.sleep(1.5)