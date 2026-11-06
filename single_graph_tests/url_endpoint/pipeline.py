import json
from pathlib import Path
import argparse
import logging
import time
import requests
import hashlib

from anacostia.streams.filesystem import DirectoryStream
from anacostia.streams.url import URLEndpointStream
from anacostia.transports.base import BaseTransport
from anacostia.consumer import Consumer
from anacostia.producer import Producer
from anacostia.node import Stage
from anacostia.dag import Graph
from anacostia.utils.debug import attach_debugger
from anacostia.utils.logging import log
from anacostia.utils.types import Artifact



parser = argparse.ArgumentParser(description="Run the pipeline after restart test")
parser.add_argument("-d", "--debug", action="store_true", help="Flag to indicate if debugging is enabled")
args = parser.parse_args()

if args.debug:
    # To debug this test:
    # Add a breakpoint by clicking on the left side of the line number you want to break on.
    # run the script: python pipeline1.py -d
    # open the debug tab in vscode
    # select the "Port 5678 - Remote Attach" configuration, then click on the play button.
    # The script will pause at the breakpoint and you can inspect the values of variables in the debug console.
    attach_debugger()

# 1. Set up streams, consumers, and nodes
testing_artifacts_dir = Path("./testing_artifacts")
pipeline1_dir = testing_artifacts_dir / "pipeline1"
pipeline2_receiving_dir = testing_artifacts_dir / "pipeline2" / ".anacostia" / "receiving"
db_folder_path = pipeline1_dir / ".anacostia"
input_path1 = pipeline1_dir / "incoming1"
producer_path = pipeline1_dir / "producer_dir"
transport_package_dir = pipeline1_dir / "transport_dir"

log_path = pipeline1_dir / "pipeline1.log"
combined_log_path = testing_artifacts_dir / "combined.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - "Pipeline1" - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler(log_path, mode='a'),
        logging.FileHandler(combined_log_path, mode='a'),
    ]
)
logger = logging.getLogger(__name__)


class CustomURLStream(URLEndpointStream):
    def __init__(self, name: str, url: str, poll_interval: float = 0.1, hash_chunk_size: int = 1_048_576, logger: logging.Logger = None):
        super().__init__(name=name, url=url, poll_interval=poll_interval, hash_chunk_size=hash_chunk_size, logger=logger)

        self.health_check_url = f"{url}/health"
        self.data_url = f"{url}/data"

    def __iter__(self):
        while True:

            try:
                data_response = requests.get(self.data_url, timeout=5)

                if data_response.status_code == 200:
                    data = data_response.json()
                    items = data.get("items", [])
                    content = json.dumps(data)

                    artifact_location = {
                        "url": self.data_url,
                        "min_id": items[0]["id"] if items else None,
                        "max_id": items[-1]["id"] if items else None,
                    }

                    artifact_hash = hashlib.sha256(str(content).encode()).hexdigest()

                    artifact = Artifact(
                        location=artifact_location,
                        hash=artifact_hash
                    )
                    self.register_artifact(
                        artifact, 
                        metadata={"min_id": artifact_location["min_id"], "max_id": artifact_location["max_id"]}, 
                        content=content
                    )
                    yield artifact

                else:
                    log(f"Data endpoint failed with status code: {data_response.status_code}", level="error", logger=self.logger)

            except requests.RequestException as e:
                log(f"Data request failed: {e}", level="error", logger=self.logger)

            # IMPORTANT: prevent polling from blocking the main thread.
            time.sleep(self.poll_interval)


# Assemble the first stage of the pipeline with a stream, consumer, producer, and transport
stream = CustomURLStream(name="url_stream", url="http://127.0.0.1:8000", logger=logger, poll_interval=5.0)
stream_consumer_odd = Consumer(name="stream_consumer_odd", stream=stream, logger=logger)
producer = Producer(name="test_producer", directory=producer_path, logger=logger)
stage = Stage(name="TestNode", consumers=[stream_consumer_odd], producers=[producer], logger=logger)

# 2. Create the graph with the node
graph = Graph(name="Pipeline1", nodes=[stage], db_folder=db_folder_path, logger=logger)

# 3. Define the node's processing function
@stage.entrypoint
def node_func():
    for bundle in stream_consumer_odd:
        with stage.stage_run() as staging_directory:
            # Process the bundle and write to the staging directory
            for artifact in bundle:
                artifact_hash = artifact.hash

                content = stream.get_artifact_content(artifact_hash)
                content = json.loads(content)
                items = [item["name"] for item in content["items"]]

                # Write the content to a file in the staging directory
                output_file_path = staging_directory / f"output_{stage.run_id}.txt"
                with open(output_file_path, 'w') as f:
                    for item in items:
                        f.write(str(item) + "\n")

                # Commit the output artifact to the producer
                committed_path = producer_path / f"output_{stage.run_id}.txt"
                committed_artifact = producer.commit_artifact(
                    artifact_staging_path=output_file_path,
                    artifact_final_path=committed_path
                ) 

                log(f"Processed items: [{items}] content written to: {output_file_path}", level="info", logger=logger)


# 3. Start the pipeline
graph.start()
try:
    graph.join()
except KeyboardInterrupt:
    graph.stop()