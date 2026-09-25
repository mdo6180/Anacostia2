from pathlib import Path
import argparse
import shutil
import logging

from anacostia.streams.filesystem import DirectoryStream
from anacostia.consumer import Consumer
from anacostia.node import Stage
from anacostia.dag import Graph
from anacostia.utils.debug import attach_debugger



# 1. Set up streams, consumers, and nodes
tests_path = Path("./testing_artifacts") / "pipeline2"
db_folder_path = tests_path / ".anacostia"
input_path1 = tests_path / "incoming1"

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
node = Stage(name="TestNode", consumers=[stream_consumer_odd], logger=logger)

# 2. Create the graph with the node
graph = Graph(name="TestGraph", nodes=[node], db_folder=db_folder_path, logger=logger)

# 3. Define the node's processing function
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

# 4. Start the pipeline
graph.start()
try:
    graph.join()
except KeyboardInterrupt:
    graph.stop()