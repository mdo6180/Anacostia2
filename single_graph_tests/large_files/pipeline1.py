from pathlib import Path
import argparse
import shutil
import logging

from anacostia.streams.filesystem import DirectoryStream
from anacostia.transports.base import BaseTransport
from anacostia.consumer import Consumer
from anacostia.producer import Producer
from anacostia.node import Stage
from anacostia.dag import Graph
from anacostia.utils.debug import attach_debugger
from anacostia.utils.logging import log



# 1. Set up streams, consumers, and nodes
tests_path = Path("./testing_artifacts") / "pipeline1"
db_folder_path = tests_path / ".anacostia"
input_path1 = tests_path / "incoming1"
producer_path = tests_path / "producer_dir"
transport_package_dir = tests_path / "transport_dir"

parser = argparse.ArgumentParser(description="Run the pipeline after restart test")
parser.add_argument("-r", "--restart", action="store_true", help="Flag to indicate if this is a restart")
parser.add_argument("-d", "--debug", action="store_true", help="Flag to indicate if debugging is enabled")
args = parser.parse_args()

if args.debug:
    # To debug this test:
    # Add a breakpoint by clicking on the left side of the line number you want to break on.
    # run the script: python mid_stream_stop.py -r -d
    # open the debug tab in vscode
    # select the "Python Debugger: Remote Attach" configuration, then click on the play button.
    # The script will pause at the breakpoint and you can inspect the values of variables in the debug console.
    attach_debugger()

if args.restart == False:
    if tests_path.exists() is True:
        shutil.rmtree(tests_path)
    tests_path.mkdir(parents=True, exist_ok=True)

log_path = tests_path / "anacostia.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=str(log_path),
    filemode='a'
)
logger = logging.getLogger(__name__)

# Assemble the first stage of the pipeline with a stream, consumer, producer, and transport
stream = DirectoryStream(name="odd_folder", directory=input_path1, logger=logger)
stream_consumer_odd = Consumer(name="stream_consumer_odd", stream=stream, logger=logger)
producer = Producer(name="test_producer", directory=producer_path, logger=logger)
simple_transport = BaseTransport(name="combined_transport", transfers_directory=transport_package_dir, logger=logger)
node = Stage(name="TestNode", consumers=[stream_consumer_odd], producers=[producer], transports=[simple_transport], logger=logger)

# 2. Create the graph with the node
graph = Graph(name="TestGraph", nodes=[node], db_folder=db_folder_path, logger=logger)

def create_large_file(path: str, size_mb: int = 10):
    target_size = size_mb * 1024 * 1024  # bytes
    line = "The quick brown fox jumps over the lazy dog.\n"

    with open(path, "a", encoding="utf-8") as f:
        while f.tell() < target_size:
            f.write(line)

        # Trim to exactly the target size
        f.truncate(target_size)

# 2. Define the node's processing function
@node.entrypoint
def node_func():
    for bundle in stream_consumer_odd:
        with node.stage_run() as staging_directory:
            artifact_obj = bundle[0]
            artifact_location = artifact_obj.location
            input_artifact_path = artifact_location["path"]

            with open(input_artifact_path, "r") as input_file:
                content = input_file.read()
                logger.info(f"processing artifact with content '{content}' in run {node.run_id} with location {input_artifact_path}")

                output_file_path = staging_directory / "output.txt"     # Define the output file path in the staging directory

                with open(output_file_path, "w") as output_file:        # Write the content to the output file in the staging directory
                    output_file.write(content)
                    output_file.write("\n")  # Add a newline for clarity

                create_large_file(path=output_file_path, size_mb=10)    # Append to create a 10MB file in the staging directory

                # Commit the output artifact to the producer
                committed_path = producer_path / f"output_{node.run_id}.txt"
                committed_artifact = producer.commit_artifact(
                    artifact_staging_path=output_file_path,
                    artifact_final_path=committed_path
                )              

            # copy the committed artifact to the transport package's data folder
            with simple_transport.create_transfer_package() as data_folder_path:
                simple_transport.add_to_package(
                    artifact_hash=committed_artifact.hash,
                    src_path=committed_path,
                    dest_path=data_folder_path / committed_path.name,
                    dest_pipeline_name="some_pipeline_name",  # This is a placeholder; replace with the actual destination pipeline name if needed
                    dest_stream="some_other_stream"  # This is a placeholder; replace with the actual destination stream name if needed
                )

# 3. Start the pipeline
graph.start()
try:
    graph.join()
except KeyboardInterrupt:
    graph.stop()