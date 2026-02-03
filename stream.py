import os
import time
import threading
import queue
from typing import Callable, Any, Optional, Tuple, List


input_path1 = "./testing_artifacts/incoming1"
input_path2 = "./testing_artifacts/incoming2"


class DirectoryStream:
    def __init__(self, directory: str, poll_interval: float = 1.0):
        if os.path.exists(directory) is False:
            print(f"Directory {directory} does not exist. Creating it.")
            os.makedirs(directory)

        self.directory = directory
        self.poll_interval = poll_interval
        self.seen = set()
        self.connection = None

    def set_db_connection(self, connection):
        self.connection = connection

    def load_artifact(self, path: str) -> str:
        with open(path, "r") as file:
            return file.read()

    def poll(self):
        """
        Yields single items: (path, content)
        """
        while True:
            for filename in sorted(os.listdir(self.directory)):
                if filename in self.seen:
                    continue

                path = os.path.join(self.directory, filename)
                if not os.path.isfile(path):
                    continue

                self.seen.add(filename)
                content = self.load_artifact(path)
                yield path, content

            time.sleep(self.poll_interval)


class StreamRunner:
    def __init__(
        self,
        stream: DirectoryStream,
        batch_size: int = 1,
        maxsize: int = 0,
        filter_func: Optional[Callable[[Any], bool]] = None,
    ):
        if batch_size <= 0:
            raise ValueError("batch_size must be >= 1")

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

            for path, item in self.stream.poll():
                if self._stop.is_set():
                    break

                # Apply filtering function if provided
                if self.filter_func is not None:
                    if not self.filter_func(item):
                        print(f"StreamRunner filtering out item: {item} from {path}")  # ignore_artifact DB call in future
                        continue
                    else:
                        print(f"StreamRunner accepting item: {item} from {path}")

                # Item accepted (or no filter)
                batch_paths.append(path)
                batch_items.append(item)

                # Emit only when batch is full
                if len(batch_items) >= self.batch_size:
                    self.items_queue.put((batch_paths, batch_items), block=True)  # backpressure here
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
            print(f"StreamRunner registering paths to DB: {batch_paths}")  # prime_artifact DB call in future
            yield batch_items


if __name__ == "__main__":
    try:
        def filter_odd(content: str) -> bool:
            return int(content[-1]) % 2 != 0  # Keep only artifacts with last character as odd number

        def filter_even(content: str) -> bool:
            return int(content[-1]) % 2 == 0  # Keep only artifacts with last character as even number

        runner1 = StreamRunner(DirectoryStream(input_path1), batch_size=2, filter_func=filter_odd).start()
        runner2 = StreamRunner(DirectoryStream(input_path2), batch_size=2, filter_func=filter_even).start()

        for batch1, batch2 in zip(runner1, runner2):
            print(f"New file detected: {batch1}, {batch2}")

    except KeyboardInterrupt:
        print("Stopping stream runners...")
        runner1.stop()
        runner2.stop()
