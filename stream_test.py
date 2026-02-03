import time


def create_file(file_path, content):
    try:
        with open(file_path, 'w') as file:
            file.write(content)
        print(f"File '{file_path}' created successfully.")
    except Exception as e:
        print(f"Error creating the file: {e}")


if __name__ == "__main__":
    tests_path = f"./testing_artifacts"
    data_store1_input = f"{tests_path}/incoming1"
    data_store2_input = f"{tests_path}/incoming2"

    for i in range(12):
        create_file(f"{data_store1_input}/test_file{i}.txt", f"incoming1 {i}")
        time.sleep(1.5)
        create_file(f"{data_store2_input}/test_file{i}.txt", f"incoming2 {i}")
        time.sleep(1.5)