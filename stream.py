from abc import ABC
import hashlib
import os
import time
import threading
import queue
import argparse
import shutil
from contextlib import contextmanager
from typing import Callable, Any, Optional, List, Generator


input_path1 = "./testing_artifacts/incoming1"
input_path2 = "./testing_artifacts/incoming2"
output_path1 = "./testing_artifacts/processed1"
output_path2 = "./testing_artifacts/processed2"
output_combined_path = "./testing_artifacts/processed_combined"

parser = argparse.ArgumentParser(description="Run the pipeline after restart test")
parser.add_argument("-r", "--restart", action="store_true", help="Flag to indicate if this is a restart")
args = parser.parse_args()

if args.restart == False:
    if os.path.exists(input_path1):
        shutil.rmtree(input_path1)
    if os.path.exists(input_path2):
        shutil.rmtree(input_path2)
    if os.path.exists(output_path1):
        shutil.rmtree(output_path1)
    if os.path.exists(output_path2):
        shutil.rmtree(output_path2)
    if os.path.exists(output_combined_path):
        shutil.rmtree(output_combined_path)


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
        # associate the file paths with file hashes in the DB for this stream
    
    def hash_file(self, filepath: str) -> str:
        sha256 = hashlib.sha256()
        with open(filepath, 'rb') as f:
            while chunk := f.read(self.hash_chunk_size):
                sha256.update(chunk)
        return sha256.hexdigest()

    def __iter__(self) -> Generator[str, Any, str]:
        """
        Yields single items: (content, file_hash)
        """
        while True:
            for filename in sorted(os.listdir(self.directory)):

                if filename in self.seen:   # replace with artifact_exists DB check in the future
                    continue

                path = os.path.join(self.directory, filename)
                if not os.path.isfile(path):
                    continue

                self.seen.add(filename)     # replace with register_artifact DB call in the future

                file_hash = self.hash_file(path)

                # Read file content, user will implement their own logic to extract the content from the artifact
                with open(path, "r") as file:
                    content = file.read()
                    yield content, file_hash

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
            bundle_items: List[Any] = []
            bundle_hashes: List[str] = []

            for item, file_hash in self.stream:
                if self._stop.is_set():
                    break

                # Apply filtering function if provided
                if self.filter_func is not None:
                    if not self.filter_func(item):
                        print(f"{self.name} ignore_artifact: {item}")       # ignore_artifact DB call in future
                        continue
                    else:
                        print(f"{self.name} prime_artifact: {item}")        # prime_artifact DB call in future

                # Item accepted (or no filter)
                bundle_items.append(item)
                bundle_hashes.append(file_hash)

                # Emit only when batch is full
                if len(bundle_items) >= self.bundle_size:
                    self.items_queue.put((bundle_items, bundle_hashes), block=True)        # backpressure here, blocks if queue is full
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
            bundle_items, bundle_hashes = self.items_queue.get(block=True)
            self.bundle_hashes = bundle_hashes  # store the hashes of the current bundle for the using_artifacts and commit_artifacts calls in the Node
            yield bundle_items


class Producer:
    def __init__(self, name: str, directory: str):
        self.name = name
        self.directory = directory
        if os.path.exists(directory) is False:
            print(f"Directory {directory} does not exist. Creating it.")
            os.makedirs(directory)
    
    def write(self, filename: str, content: str):
        path = os.path.join(self.directory, filename)
        with open(path, "a") as file:
            file.write(content)
        print(f"{self.name} produced artifact: {path}")    # produced_artifact DB call in future


class Node(threading.Thread, ABC):
    def __init__(self, name: str, consumers: List[Consumer], producers: List[Producer]):
        self.consumers = consumers
        self.producers = producers
        self.run_id = 0
        
        self.stream_consumer_odd = consumers[0]
        self.stream_consumer_even = consumers[1]

        self.odd_producer = producers[0]
        self.even_producer = producers[1]
        self.combined_producer = producers[2]

        super().__init__(name=name, daemon=True)
    
    def start_consumers(self):
        for consumer in self.consumers:
            consumer.start()
    
    def stop_consumers(self):
        for consumer in self.consumers:
            consumer.stop()
    
    def using_artifacts(self):
        # placeholder for logic to mark artifacts as being used in the DB
        hashes = []
        for consumer in self.consumers:
            hashes.extend(consumer.bundle_hashes)
        print(f"{self.name} using_artifact: {hashes}")     # using_artifact DB call in future
    
    def commit_artifacts(self):
        # placeholder for logic to mark artifacts as committed in the DB
        hashes = []
        for consumer in self.consumers:
            hashes.extend(consumer.bundle_hashes)
        print(f"{self.name} committed_artifact: {hashes}")     # commit_artifact DB call in future

    @contextmanager
    def stage_run(self):
        try:
            print(f"\nNode {self.name} starting run {self.run_id}")   # start_run DB call in future
            self.using_artifacts()    # mark artifacts as being used in the DB
            
            yield
            
            self.commit_artifacts()   # mark artifacts as committed in the DB
            print(f"Node {self.name} finished run {self.run_id}\n")    # end_run DB call in future
            self.run_id += 1

        except Exception as e:
            print(f"Error in node {self.name} during run {self.run_id}: {e}")
            # log the error in DB here, used to display run error on GUI

    def run(self, func):
        try:
            self.start_consumers()
            # get the max run_id from the DB for this node and set self.run_id to max_run_id + 1 here, so that run IDs are consistent across restarts
            func()

        except Exception as e:
            print(f"Error in node {self.name}: {e}")
            # log the error in DB here, used to display run error on GUI

        except KeyboardInterrupt:
            print(f"Node {self.name} received KeyboardInterrupt. Stopping...")
            self.stop_consumers()


if __name__ == "__main__":
    # Test 3: Two DirectoryStreams with bundle_size=2 and filtering functions
    def filter_odd(content: str) -> bool:
        return int(content[-1]) % 2 != 0    # Keep only artifacts with last character as odd number

    def filter_even(content: str) -> bool:
        return int(content[-1]) % 2 == 0    # Keep only artifacts with last character as even number

    stream_consumer_odd = Consumer(name="Stream1", stream=DirectoryStream(input_path1), bundle_size=2, filter_func=filter_odd)
    stream_consumer_even = Consumer(name="Stream2", stream=DirectoryStream(input_path2), bundle_size=2, filter_func=filter_even)
    odd_producer = Producer(name="Producer1", directory=output_path1)   # example producer, not used in this test
    even_producer = Producer(name="Producer2", directory=output_path2)   # example producer, not used in this test
    combined_producer = Producer(name="CombinedProducer", directory=output_combined_path)   # example producer to write combined results, not used in this test

    node = Node(name="TestNode", consumers=[stream_consumer_odd, stream_consumer_even], producers=[odd_producer, even_producer, combined_producer])
    
    @node.run
    def node_func():
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

        for bundle1, bundle2 in zip(stream_consumer_odd, stream_consumer_even):
            with node.stage_run():
                for item1, item2 in zip(bundle1, bundle2):
                    odd_producer.write(filename=f"processed_odd_{node.run_id}.txt", content=f"Processed {item1} from odd stream\n")
                    even_producer.write(filename=f"processed_even_{node.run_id}.txt", content=f"Processed {item2} from even stream\n")
                    combined_producer.write(filename=f"processed_combined_{node.run_id}.txt", content=f"Processed {item1} and {item2} from combined streams\n")

    node.start()
    node.join()