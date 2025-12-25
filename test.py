import os
import shutil
import logging
import time

from nodes.stage import BaseStageNode
from nodes.watcher import BaseWatcherNode
from pipeline.pipeline import Pipeline



# Create the testing artifacts directory for the SQLAlchemy tests
tests_path = "./testing_artifacts"
if os.path.exists(tests_path) is True:
    shutil.rmtree(tests_path)
os.makedirs(tests_path)
data_store_path1 = f"{tests_path}/data_store1"
data_store_path2 = f"{tests_path}/data_store2"
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
    def __init__(self, name, path, hash_chunk_size = 1048576, logger = None):
        super().__init__(name, path, hash_chunk_size, logger)

    def resource_trigger(self) -> None:
        if len(self.get_unused_artifacts()) > 0:
            self.trigger(message="New artifact detected")
    
    def execute(self):
        unused_artifacts = self.get_unused_artifacts()
        for artifact, hash in unused_artifacts:
            self.log(f"{self.name} processing artifact: {artifact} with hash: {hash}", level="INFO")
            self.mark_artifact_used(artifact, hash)
            self.log(f"{self.name} finished processing artifact: {artifact}", level="INFO")


class DelayStageNode(BaseStageNode):
    def __init__(self, name, predecessors = None, delay: int = 3, logger = None):
        super().__init__(name, predecessors, logger)
        self.delay = delay

    def execute(self):
        self.log(f"{self.name} started delay of {self.delay} seconds", level="INFO")
        time.sleep(self.delay)
        self.log(f"{self.name} finished delay of {self.delay} seconds", level="INFO")



if __name__ == "__main__":
    # Example usage of BaseStageNode and BaseWatcherNode
    watcher_node = FolderWatcherNode(name="WatcherNode1", path=data_store_path1, logger=logger)
    watcher_node2 = FolderWatcherNode(name="WatcherNode2", path=data_store_path2, logger=logger)
    stage_node = DelayStageNode(name="StageNode1", predecessors=[watcher_node, watcher_node2], delay=4, logger=logger)
    stage_node2 = DelayStageNode(name="StageNode2", predecessors=[stage_node], delay=2, logger=logger)

    pipeline = Pipeline(name="TestPipeline", nodes=[watcher_node, watcher_node2, stage_node, stage_node2], db_folder=db_folder_path, logger=logger)
    try:
        pipeline.launch_nodes()
    except KeyboardInterrupt:
        print("KeyboardInterrupt received. Terminating pipeline...")
        pipeline.terminate_nodes()
    
    # Note: in theory we should see watcher_node2 triggering twice for every time watcher_node1 triggers 
    # and then stage_node should trigger after both watchers have triggered.