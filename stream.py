import os
import time
import threading
import queue
import argparse
import shutil
from typing import Callable, Any, Optional, List


input_path1 = "./testing_artifacts/incoming1"
input_path2 = "./testing_artifacts/incoming2"

parser = argparse.ArgumentParser(description="Run the pipeline after restart test")
parser.add_argument("-r", "--restart", action="store_true", help="Flag to indicate if this is a restart")
args = parser.parse_args()

if args.restart == False:
    if os.path.exists(input_path1):
        shutil.rmtree(input_path1)
    if os.path.exists(input_path2):
        shutil.rmtree(input_path2)


class DirectoryStream:
    def __init__(self, directory: str, poll_interval: float = 0.1):
        if os.path.exists(directory) is False:
            print(f"Directory {directory} does not exist. Creating it.")
            os.makedirs(directory)

        self.directory = directory
        self.poll_interval = poll_interval
        self.seen = set()
        self.connection = None

    def set_db_connection(self, connection):
        self.connection = connection
        # add logic to create necessary tables if needed

    def __iter__(self):
        """
        Yields single items: (path, content)
        """
        while True:
            for filename in sorted(os.listdir(self.directory)):

                if filename in self.seen:   # replace with artifact_exists DB check in the future
                    continue

                path = os.path.join(self.directory, filename)
                if not os.path.isfile(path):
                    continue

                self.seen.add(filename)     # replace with register_artifact DB call in the future

                # Read file content, user will implement their own logic to extract the content from the artifact
                with open(path, "r") as file:
                    content = file.read()
                    yield path, content

            # IMPORTANT: put this sleep here so the polling doesn't block the main thread.
            time.sleep(self.poll_interval)


class StreamRunner:
    def __init__(
        self,
        name: str,
        stream: DirectoryStream,
        batch_size: int = 1,
        maxsize: int = 0,
        filter_func: Optional[Callable[[Any], bool]] = None,
    ):
        if batch_size <= 0:
            raise ValueError("batch_size must be >= 1")

        self.name = name
        self.stream = stream
        self.batch_size = batch_size
        self.db_connection = None                           # placeholder for DB connection
        self.stream.set_db_connection(self.db_connection)   # set to actual DB connection in the future
        self.filter_func = filter_func

        self.items_queue = queue.Queue(maxsize=maxsize)
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        def run():
            batch_paths: List[str] = []
            batch_items: List[Any] = []

            for path, item in self.stream:
                if self._stop.is_set():
                    break

                # Apply filtering function if provided
                if self.filter_func is not None:
                    if not self.filter_func(item):
                        print(f"{self.name} ignore_artifact: {item} from {path}")       # ignore_artifact DB call in future
                        continue
                    else:
                        print(f"{self.name} prime_artifact: {item} from {path}")        # prime_artifact DB call in future

                # Item accepted (or no filter)
                batch_paths.append(path)
                batch_items.append(item)

                # Emit only when batch is full
                if len(batch_items) >= self.batch_size:
                    self.items_queue.put((batch_paths, batch_items), block=True)        # backpressure here, blocks if queue is full
                    batch_paths = []
                    batch_items = []

            # Optional: decide whether to flush partial batch on stop.
            # Current behavior: do NOT flush partial batch.

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()

    def __iter__(self):
        while not self._stop.is_set():
            batch_paths, batch_items = self.items_queue.get(block=True)
            print(f"{self.name} using_artifact: {batch_paths}")     # using_artifact DB call in future
            yield batch_items


if __name__ == "__main__":
    try:
        """
        # Test 0: Single DirectoryStream with batch_size=1
        runner1 = StreamRunner(name="Stream1", stream=DirectoryStream(input_path1)).start()
        for batch1 in runner1:
            for item1 in batch1:
                print(f"New file detected: {item1}")

        # Test 1: Single DirectoryStream with batch_size=2
        runner1 = StreamRunner(name="Stream1", stream=DirectoryStream(input_path1, batch_size=2)).start()
        for batch1 in runner1:
            for item1 in batch1:
                print(f"New file detected: {item1}")
        
        # Test 2: Two DirectoryStreams with batch_size=2
        runner1 = StreamRunner(name="Stream1", stream=DirectoryStream(input_path1, batch_size=2)).start()
        runner2 = StreamRunner(name="Stream2", stream=DirectoryStream(input_path2, batch_size=2)).start()
        for batch1, batch2 in zip(runner1, runner2):
            for item1, item2 in zip(batch1, batch2):
                print(f"New file detected: {item1}, {item2}")

        """

        # Test 3: Two DirectoryStreams with batch_size=2 and filtering functions
        def filter_odd(content: str) -> bool:
            return int(content[-1]) % 2 != 0    # Keep only artifacts with last character as odd number

        def filter_even(content: str) -> bool:
            return int(content[-1]) % 2 == 0    # Keep only artifacts with last character as even number

        runner1 = StreamRunner(name="Stream1", stream=DirectoryStream(input_path1), batch_size=2, filter_func=filter_odd).start()
        runner2 = StreamRunner(name="Stream2", stream=DirectoryStream(input_path2), batch_size=2, filter_func=filter_even).start()

        for batch1, batch2 in zip(runner1, runner2):
            for item1, item2 in zip(batch1, batch2):
                #print(f"using_artifact: {item1}, {item2}")             # using_artifact DB call in future
                print(f"processing artifacts detected: {item1}, {item2}")
                #print(f"used_artifact: {item1}, {item2}")         # used_artifact DB call in future

    except KeyboardInterrupt:
        print("Stopping stream runners...")
        runner1.stop()
        runner2.stop()
