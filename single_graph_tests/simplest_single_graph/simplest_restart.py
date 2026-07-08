from pathlib import Path
import argparse
import shutil
import logging

from anacostia.streams.filesystem import DirectoryStream
from anacostia.consumer import Consumer
from anacostia.node import Node
from anacostia.dag import Graph

from anacostia.utils.debug import stop_if



# 1. Set up streams, consumers, and nodes
tests_path = Path("./testing_artifacts")
db_folder_path = tests_path / ".anacostia"
input_path1 = tests_path / "incoming1"

parser = argparse.ArgumentParser(description="Run the pipeline after restart test")
parser.add_argument("-r", "--restart", action="store_true", help="Flag to indicate if this is a restart")
args = parser.parse_args()

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

stream = DirectoryStream(name="odd_folder", directory=input_path1, logger=logger)
stream_consumer_odd = Consumer(name="stream_consumer_odd", stream=stream, logger=logger)
node = Node(name="TestNode", consumers=[stream_consumer_odd], logger=logger)

# 2. Define the node's processing function
@node.entrypoint
def node_func():
    for bundle in stream_consumer_odd:
        with node.stage_run():
            # bundle = [Artifact(location={'filepath': 'testing_artifacts/incoming1/test_file0.txt'}, hash='d41d8cd98f00b204e9800998ecf8427e')]
            artifact = bundle[0]
            file_path = artifact.location["path"]
            file_hash = artifact.hash

            with open(file_path, "r") as f:
                content = f.read()
                logger.info(f"processing artifact with content '{content}' in run {node.run_id} with location {artifact.location} and hash {file_hash}")

                if args.restart == False:
                    stop_if(current_run=node.run_id, current_iter=0, target_run=5, target_iter=0, mode="sigint", logger=logger) 

# 3. Create and start the graph
graph = Graph(name="TestGraph", nodes=[node], db_folder=db_folder_path, logger=logger)

graph.start()
try:
    graph.join()
except KeyboardInterrupt:
    graph.stop()