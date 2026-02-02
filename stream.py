import os
import time
import threading
import queue



input_path1 = "./testing_artifacts/incoming1"
input_path2 = "./testing_artifacts/incoming2"


class DirectoryStream:
    def __init__(self, directory: str, poll_interval: float = 1.0, batch_size: int = 1):
        if os.path.exists(directory) is False:
            print(f"Directory {directory} does not exist. Creating it.")
            os.makedirs(directory)

        if batch_size <= 0:
            raise ValueError("batch_size must be >= 1")

        self.directory = directory
        self.poll_interval = poll_interval
        self.batch_size = batch_size
        self.seen = set()
    
    def set_db_connection(self, connection):
        self.connection = connection

    def poll(self):
        batch_paths = []
        batch_content = []

        while True:
            for filename in sorted(os.listdir(self.directory)):
                # Skip files we've already seen
                # replace with artifact_exists DB check in the future
                if filename in self.seen:
                    continue

                # Skip if not a file (e.g., sub-directory)
                path = os.path.join(self.directory, filename)
                if not os.path.isfile(path):
                    continue

                self.seen.add(filename)
                batch_paths.append(path)    # replace with register_artifact DB call in the future

                # Read file content, user will implement their own logic to extract the content from the artifact
                with open(path, 'r') as file:
                    content = file.read()
                    batch_content.append(content)

                # Only yield when the batch is FULL
                if len(batch_paths) >= self.batch_size:
                    yield batch_paths, batch_content
                    batch_paths = []
                    batch_content = []

            # IMPORTANT: do NOT yield partial batches here.
            time.sleep(self.poll_interval)


class StreamRunner:
    def __init__(self, stream: DirectoryStream, maxsize=0):
        self.stream = stream
        self.db_connection = None                           # placeholder for DB connection
        self.stream.set_db_connection(self.db_connection)   # set to actual DB connection in the future

        self.items_queue = queue.Queue(maxsize=maxsize)
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        def run():
            for item in self.stream.poll():
                if self._stop.is_set():
                    break
                self.items_queue.put(item=item, block=True)  # backpressure here, blocks if queue is full
        self._thread = threading.Thread(target=run, daemon=True)
        self._thread.start()
        return self

    def stop(self):
        self._stop.set()

    def __iter__(self):
        while not self._stop.is_set():
            batch_paths, batch_content = self.items_queue.get(block=True)   # blocks until item is available
            print(f"StreamRunner registering paths to DB: {batch_paths}")   # replace with prime_artifact DB call in the future
            yield batch_content


if __name__ == "__main__":
    try:
        """
        # Test 0: Single DirectoryStream with batch_size=1
        runner1 = StreamRunner(DirectoryStream(input_path1)).start()
        for batch1 in runner1:
            print(f"New file detected: {batch1}")

        # Test 1: Single DirectoryStream with batch_size=2
        runner1 = StreamRunner(DirectoryStream(input_path1, batch_size=2)).start()
        for batch1 in runner1:
            print(f"New file detected: {batch1}")
        
        # Test 2: Two DirectoryStreams with batch_size=2
        runner1 = StreamRunner(DirectoryStream(input_path1, batch_size=2)).start()
        runner2 = StreamRunner(DirectoryStream(input_path2, batch_size=2)).start()
        for batch1, batch2 in zip(runner1, runner2):
            print(f"New file detected: {batch1}, {batch2}")

        """
        runner1 = StreamRunner(DirectoryStream(input_path1, batch_size=2)).start()
        runner2 = StreamRunner(DirectoryStream(input_path2, batch_size=2)).start()

        for batch1, batch2 in zip(runner1, runner2):
            print(f"New file detected: {batch1}, {batch2}")

    except KeyboardInterrupt:
        print("Stopping stream runners...")
        runner1.stop()
        runner2.stop()