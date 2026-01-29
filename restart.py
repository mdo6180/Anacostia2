import os
import shutil
import logging
import time
import argparse

from nodes.stage import BaseStageNode
from nodes.watcher import BaseWatcherNode, ConsumeInput, ProduceOutput
from pipeline.pipeline import Pipeline


parser = argparse.ArgumentParser(description="Run the pipeline after restart test")
parser.add_argument("-r", "--restart", action="store_true", help="Flag to indicate if this is a restart")
args = parser.parse_args()


# Create the testing artifacts directory for the SQLAlchemy tests
tests_path = "./testing_artifacts"

if args.restart == False:
    if os.path.exists(tests_path) is True:
        shutil.rmtree(tests_path)
    os.makedirs(tests_path)

data_store1_input = f"{tests_path}/data_store1_input"
data_store1_output = f"{tests_path}/data_store1_output"
data_store2_input = f"{tests_path}/data_store2_input"
data_store2_output = f"{tests_path}/data_store2_output"
db_folder_path = f"{tests_path}/.anacostia"

log_path = f"{tests_path}/anacostia.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='a'
)
logger = logging.getLogger(__name__)



class FolderWatcherNode(BaseWatcherNode):
    def __init__(self, name, input_path, output_path, logger = None):
        super().__init__(name=name, input_path=input_path, output_path=output_path, max_artifacts_per_run=1, hash_chunk_size=1048576, logger=logger)

    def execute(self):
        # Process one artifact at a time
        artifact_path, hash = self.get_filtered_artifacts()[0]
        with ConsumeInput(node=self, filename=os.path.basename(artifact_path), artifact_hash=hash) as file:
            content = file.read()
            self.log(f"{self.name} processing artifact: {artifact_path} | hash: {hash} | content: {content}", level="INFO")
            print(f"{self.name} processing artifact: {artifact_path} | hash: {hash} | content: {content}")
            time.sleep(1.5)  # Simulate some processing time
            print(f"{self.name} finished processing artifact: {artifact_path}")
            self.log(f"{self.name} finished processing artifact: {artifact_path}", level="INFO")
        
        outfile_name = os.path.basename(artifact_path)
        outfile_name = f"processed_{outfile_name}"
        with ProduceOutput(node=self, filename=outfile_name, mode='w') as outfile:
            self.log(f"{self.name} writing output artifact: {os.path.join(self.output_path, outfile_name)}", level="INFO")
            print(f"{self.name} writing output artifact: {os.path.join(self.output_path, outfile_name)}")
            outfile.write(content)
            outfile.write(f"\nProcessed by {self.name}\n")
            time.sleep(1.5)  # Simulate some processing time
            print(f"{self.name} wrote output artifact: {os.path.join(self.output_path, outfile_name)}")
            self.log(f"{self.name} wrote output artifact: {os.path.join(self.output_path, outfile_name)}", level="INFO")


class DelayStageNode(BaseStageNode):
    def __init__(self, name, predecessors = None, delay: int = 3, logger = None):
        super().__init__(name, predecessors, logger)
        self.delay = delay

    def execute(self):
        self.log(f"{self.name} started delay of {self.delay} seconds", level="INFO")
        time.sleep(self.delay)
        self.log(f"{self.name} finished delay of {self.delay} seconds", level="INFO")



if __name__ == "__main__":
    def filter_odd(artifact):
        with open(artifact, 'r') as f:
            content = f.read()
            return int(content[-1]) % 2 != 0  # Keep only artifacts with last character as odd number

    def filter_even(artifact):
        with open(artifact, 'r') as f:
            content = f.read()
            return int(content[-1]) % 2 == 0  # Keep only artifacts with last character as even number

    # Example usage of BaseStageNode and BaseWatcherNode
    watcher_node1 = FolderWatcherNode(name="WatcherNode1", input_path=data_store1_input, output_path=data_store1_output, logger=logger)
    watcher_node1.set_filtering_function(filter_odd)    # process only odd-numbered artifacts

    watcher_node2 = FolderWatcherNode(name="WatcherNode2", input_path=data_store2_input, output_path=data_store2_output, logger=logger)
    watcher_node2.set_filtering_function(filter_even)   # process only even-numbered artifacts
    
    stage_node = DelayStageNode(name="StageNode1", predecessors=[watcher_node1, watcher_node2], delay=4, logger=logger)
    stage_node2 = DelayStageNode(name="StageNode2", predecessors=[stage_node], delay=2, logger=logger)

    pipeline = Pipeline(name="TestPipeline", nodes=[watcher_node1, watcher_node2, stage_node, stage_node2], db_folder=db_folder_path, logger=logger)
    try:
        pipeline.launch_nodes()
    except KeyboardInterrupt:
        print("KeyboardInterrupt received. Terminating pipeline...")
        pipeline.terminate_nodes()
    
    # How to run the test:
    # 1. Run this script without the --restart flag in one terminal to start the pipeline.
    # 2. Add some files to data_store1_input and data_store2_input to trigger the watchers.
    # 3. After some time, stop the script (Ctrl+C).
    # 4. Run this script again with the --restart flag to simulate a restart.
    # 5. Observe the logs to verify that the pipeline resumes correctly from where it left off.

    # Note: in theory we should see watcher_node2 triggering twice for every time watcher_node1 triggers 
    # and then stage_node should trigger after both watchers have triggered.