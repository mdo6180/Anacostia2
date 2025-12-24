import os
import shutil
import logging

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



if __name__ == "__main__":
    # Example usage of BaseStageNode and BaseWatcherNode
    watcher_node = BaseWatcherNode(name="WatcherNode1", path=data_store_path1, logger=logger)
    watcher_node2 = BaseWatcherNode(name="WatcherNode2", path=data_store_path2, logger=logger)
    stage_node = BaseStageNode(name="StageNode1", predecessors=[watcher_node, watcher_node2], logger=logger)
    stage_node2 = BaseStageNode(name="StageNode2", predecessors=[stage_node], logger=logger)

    pipeline = Pipeline(name="TestPipeline", nodes=[watcher_node, watcher_node2, stage_node, stage_node2], db_folder=db_folder_path, logger=logger)
    try:
        pipeline.launch_nodes()
    except KeyboardInterrupt:
        print("KeyboardInterrupt received. Terminating pipeline...")
        pipeline.terminate_nodes()