import logging
import os
import argparse
import shutil
import time
from pathlib import Path

from streams.directory import DirectoryStream
from consumer import Consumer
from producer import Producer
from transports.local import FileSystemTransport
from node import Node
from dag import Graph
from utils.debug import stop_if

sql = str   # alias of the str type for syntax highlighting using the Python Inline Source Syntax Highlighting extension by Sam Willis in VSCode.



tests_path = "./testing_artifacts"
db_folder_path = f"{tests_path}/.anacostia"
input_path1 = f"{tests_path}/incoming1"
input_path2 = f"{tests_path}/incoming2"
output_path1 = f"{tests_path}/processed1"
output_path2 = f"{tests_path}/processed2"
output_combined_path = f"{tests_path}/processed_combined"
test_node_path = f"{tests_path}/test_node"
transport_package_dir = f"{tests_path}/transport_dir"
model_registry_path = f"{tests_path}/model_registry"

parser = argparse.ArgumentParser(description="Run the pipeline after restart test")
parser.add_argument("-r", "--restart", action="store_true", help="Flag to indicate if this is a restart")
args = parser.parse_args()

if args.restart == False:
    if os.path.exists(tests_path) is True:
        shutil.rmtree(tests_path)
    os.makedirs(tests_path)

log_path = f"{tests_path}/anacostia.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=log_path,
    filemode='a'
)
logger = logging.getLogger(__name__)


# Test 3: Two DirectoryStreams with bundle_size=2 and filtering functions
def filter_odd(content: str) -> bool:
    return int(content[-1]) % 2 != 0    # Keep only artifacts with last character as odd number

def filter_even(content: str) -> bool:
    return int(content[-1]) % 2 == 0    # Keep only artifacts with last character as even number

stream_consumer_odd = Consumer(
    name="stream_consumer_odd", 
    stream=DirectoryStream(name="odd_folder", directory=input_path1, logger=logger), 
    bundle_size=2, filter_func=filter_odd, logger=logger
)
stream_consumer_even = Consumer(
    name="stream_consumer_even", 
    stream=DirectoryStream(name="even_folder", directory=input_path2, logger=logger), 
    bundle_size=2, filter_func=filter_even, logger=logger
)
odd_producer = Producer(name="odd_producer", directory=output_path1, logger=logger)   # example producer
even_producer = Producer(name="even_producer", directory=output_path2, logger=logger)   # example producer
combined_producer = Producer(name="combined_producer", directory=output_combined_path, logger=logger)   # example producer

combined_transport = FileSystemTransport(name="combined_transport", packages_directory=transport_package_dir, logger=logger)

node = Node(
    name="TestNode", 
    consumers=[stream_consumer_odd, stream_consumer_even], 
    producers=[odd_producer, even_producer, combined_producer], 
    transports=[combined_transport],
    logger=logger
)

@node.entrypoint
def node_func():
    if node.run_id == 0:
        # if it's the first run (i.e. if it's the first time this pipeline has ever been ran), execute the normal entrypoint logic
        logger.info(f"Node {node.name} starting entrypoint function for run {node.run_id}")
    else:
        # if it's a restart, execute custom logic before resuming the entrypoint function
        logger.info(f"Node {node.name} executing custom restart logic for run {node.run_id}")

    for bundle1, bundle2 in zip(stream_consumer_odd, stream_consumer_even):
        with node.stage_run():
            odd_path = odd_producer.get_staging_directory() / f"processed_odd_{node.run_id}.txt"
            even_path = even_producer.get_staging_directory() / f"processed_even_{node.run_id}.txt"
            
            subdir = combined_producer.get_staging_directory() / f"combined_dir_{node.run_id}"
            os.makedirs(subdir, exist_ok=True)
            combined_staging_path = subdir / f"processed_combined_{node.run_id}.txt"

            for i, (item1, item2) in enumerate(zip(bundle1, bundle2)):
                with open(odd_path, "a") as file:
                    file.write(f"Processed {item1} from odd stream\n")
                
                #time.sleep(1)   # checkpoint 1
                
                with open(even_path, "a") as file:
                    file.write(f"Processed {item2} from even stream\n")
                
                time.sleep(1)   # checkpoint 2
                
                # simulate failure at run 1, iter 0 (first iteration of the second run)
                # disable this stop_if after restart to allow the pipeline to continue and finish processing
                if args.restart == False:
                    stop_if(current_run=node.run_id, current_iter=i, target_run=1, target_iter=0, mode="sigint", logger=logger) 
                
                with open(combined_staging_path, "a") as file:
                    file.write(f"Processed {item1} and {item2} from combined streams\n")
                
                #time.sleep(1)   # checkpoint 3
            
            combined_file_path, combined_file_hash = combined_producer.commit_artifact(
                artifact_staging_path=combined_staging_path, 
                artifact_final_path=combined_producer.get_final_directory() / f"processed_combined_{node.run_id}.txt"
            )

            combined_transport.stage_artifact(
                artifact_path=combined_file_path, 
                artifact_staging_path=combined_transport.get_staging_directory() / f"processed_combined_{node.run_id}.txt",
                artifact_hash=combined_file_hash
            )

            combined_transport.package()


graph = Graph(name="TestGraph", nodes=[node], db_folder=db_folder_path, logger=logger)


if __name__ == "__main__":

    graph.start()

    try:
        graph.join()
    except KeyboardInterrupt:
        print(f"Node {node.name} received KeyboardInterrupt. Stopping...")
        graph.stop()

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