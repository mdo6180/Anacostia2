import logging
import os
import argparse
import shutil
import time

from streams.directory import DirectoryStream
from producers.producer import Producer
from consumers.consumer import Consumer
from nodes.node import Node
from dag import Graph


tests_path = "./testing_artifacts"
db_folder_path = f"{tests_path}/.anacostia"
input_path1 = f"{tests_path}/incoming1"
input_path2 = f"{tests_path}/incoming2"
output_path1 = f"{tests_path}/processed1"
output_path2 = f"{tests_path}/processed2"
output_combined_path = f"{tests_path}/processed_combined"

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
    name="Stream1", 
    stream=DirectoryStream(name="odd_folder", directory=input_path1, logger=logger), 
    bundle_size=2, filter_func=filter_odd, logger=logger
)
stream_consumer_even = Consumer(
    name="Stream2", 
    stream=DirectoryStream(name="even_folder", directory=input_path2, logger=logger), 
    bundle_size=2, filter_func=filter_even, logger=logger
)
odd_producer = Producer(name="Producer1", directory=output_path1, logger=logger)   # example producer
even_producer = Producer(name="Producer2", directory=output_path2, logger=logger)   # example producer
combined_producer = Producer(name="CombinedProducer", directory=output_combined_path, logger=logger)   # example producer to write combined results

node = Node(name="TestNode", consumers=[stream_consumer_odd, stream_consumer_even], producers=[odd_producer, even_producer, combined_producer], logger=logger)

@node.entrypoint
def node_func():
    for bundle1, bundle2 in zip(stream_consumer_odd, stream_consumer_even):
        with node.stage_run():
            for item1, item2 in zip(bundle1, bundle2):
                odd_producer.write(filename=f"processed_odd_{node.run_id}.txt", content=f"Processed {item1} from odd stream\n")
                even_producer.write(filename=f"processed_even_{node.run_id}.txt", content=f"Processed {item2} from even stream\n")
                combined_producer.write(filename=f"processed_combined_{node.run_id}.txt", content=f"Processed {item1} and {item2} from combined streams\n")
            
            time.sleep(2)   # simulate some processing time

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