import hashlib
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
    def __init__(self, directory: str, poll_interval: float = 0.1, hash_chunk_size: int = 1_048_576):
        if os.path.exists(directory) is False:
            print(f"Directory {directory} does not exist. Creating it.")
            os.makedirs(directory)

        self.directory = directory
        self.poll_interval = poll_interval
        self.hash_chunk_size = hash_chunk_size
        self.seen = set()
        self.connection = None

    def set_db_connection(self, connection):
        self.connection = connection
        # add logic to create necessary tables if needed
        # create a stream_artifacts table to track seen artifacts for all streams
        # add a way to get indexes from seen artifacts to help with resuming streams after restart
    
    def hash_artifact(self, filepath: str) -> str:
        sha256 = hashlib.sha256()
        with open(filepath, 'rb') as f:
            while chunk := f.read(self.hash_chunk_size):
                sha256.update(chunk)
        return sha256.hexdigest()

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

                file_hash = self.hash_artifact(path)

                # Read file content, user will implement their own logic to extract the content from the artifact
                with open(path, "r") as file:
                    content = file.read()
                    yield path, content, file_hash

            # IMPORTANT: put this sleep here so the polling doesn't block the main thread.
            time.sleep(self.poll_interval)


class Consumer:
    def __init__(
        self,
        name: str,
        stream: DirectoryStream,
        bundle_size: int = 1,
        maxsize: int = 0,
        filter_func: Optional[Callable[[Any], bool]] = None,
    ):
        if bundle_size <= 0:
            raise ValueError("bundle_size must be >= 1")

        self.name = name
        self.stream = stream
        self.bundle_size = bundle_size
        self.db_connection = None                           # placeholder for DB connection
        self.stream.set_db_connection(self.db_connection)   # set to actual DB connection in the future
        self.filter_func = filter_func

        self.items_queue = queue.Queue(maxsize=maxsize)
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        def run():
            bundle_paths: List[str] = []
            bundle_items: List[Any] = []
            bundle_hashes: List[str] = []

            for path, item, file_hash in self.stream:
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
                bundle_paths.append(path)
                bundle_items.append(item)
                bundle_hashes.append(file_hash)

                # Emit only when batch is full
                if len(bundle_items) >= self.bundle_size:
                    self.items_queue.put((bundle_paths, bundle_items, bundle_hashes), block=True)        # backpressure here, blocks if queue is full
                    bundle_paths = []
                    bundle_items = []
                    bundle_hashes = []

            # Optional: decide whether to flush partial batch on stop.
            # Current behavior: do NOT flush partial batch.

        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()

    def __iter__(self):
        # get the last partial bundle by checking which items are primed but not yet in the use_artifact state
        # yield the last bundle when the StreamRunner is restarted here

        while not self._stop.is_set():
            bundle_paths, bundle_items, bundle_hashes = self.items_queue.get(block=True)
            #print(f"{self.name} using_artifact: {bundle_paths}")     # using_artifact DB call in future
            yield bundle_items


class Run:
    def __init__(self, run_id: int):
        self.run_id = run_id
        
    def __enter__(self):
        # mark artifacts as in using_artifact in DB here
        print(f"\nStarting run {self.run_id}")      # start_run DB call in future, used to display run started on GUI
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # mark artifacts as used_artifact in DB here
        print(f"Finished run {self.run_id}\n")   # end_run DB call in future, used to display run ended on GUI


class Artifact:
    def __init__(self, content: str):
        self.content = content
    
    def __enter__(self):
        print(f"using_artifact: {self.content}")
        return self.content
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        print(f"used_artifact: {self.content}")


if __name__ == "__main__":
    run_id = 0

    try:
        """
        # Test 0: Single DirectoryStream with bundle_size=1
        runner1 = StreamRunner(name="Stream1", stream=DirectoryStream(input_path1)).start()
        for batch1 in runner1:
            item1 = batch1[0]
            print(f"New file detected: {item1}")

        # Test 1: Single DirectoryStream with bundle_size=2
        runner1 = StreamRunner(name="Stream1", stream=DirectoryStream(input_path1, bundle_size=2)).start()
        for batch1 in runner1:
            for item1 in batch1:
                print(f"New file detected: {item1}")
        
        # Test 2: Two DirectoryStreams with bundle_size=2
        runner1 = StreamRunner(name="Stream1", stream=DirectoryStream(input_path1, bundle_size=2)).start()
        runner2 = StreamRunner(name="Stream2", stream=DirectoryStream(input_path2, bundle_size=2)).start()
        for batch1, batch2 in zip(runner1, runner2):
            for item1, item2 in zip(batch1, batch2):
                print(f"New file detected: {item1}, {item2}")

        """

        # Test 3: Two DirectoryStreams with bundle_size=2 and filtering functions
        def filter_odd(content: str) -> bool:
            return int(content[-1]) % 2 != 0    # Keep only artifacts with last character as odd number

        def filter_even(content: str) -> bool:
            return int(content[-1]) % 2 == 0    # Keep only artifacts with last character as even number

        stream_consumer_odd = Consumer(name="Stream1", stream=DirectoryStream(input_path1), bundle_size=2, filter_func=filter_odd).start()
        stream_consumer_even = Consumer(name="Stream2", stream=DirectoryStream(input_path2), bundle_size=2, filter_func=filter_even).start()

        for bundle1, bundle2 in zip(stream_consumer_odd, stream_consumer_even):
            with Run(run_id):
                for item1, item2 in zip(bundle1, bundle2):
                    with Artifact(item1) as artifact1, Artifact(item2) as artifact2:    # get rid of Artifact context manager in the future
                        print(f"processing artifacts detected: {artifact1}, {artifact2}")
                
            run_id += 1

    except KeyboardInterrupt:
        print("Stopping stream runners...")
        stream_consumer_odd.stop()
        stream_consumer_even.stop()
