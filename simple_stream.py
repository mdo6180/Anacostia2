from logging import Logger
import os
import argparse
import shutil

from streams.directory import DirectoryStream
from producers.producer import Producer
from consumers.consumer import Consumer
from nodes.node import Node


input_path1 = "./testing_artifacts/incoming1"
input_path2 = "./testing_artifacts/incoming2"
output_path1 = "./testing_artifacts/processed1"
output_path2 = "./testing_artifacts/processed2"
output_combined_path = "./testing_artifacts/processed_combined"

parser = argparse.ArgumentParser(description="Run the pipeline after restart test")
parser.add_argument("-r", "--restart", action="store_true", help="Flag to indicate if this is a restart")
args = parser.parse_args()

if args.restart == False:
    if os.path.exists(input_path1):
        shutil.rmtree(input_path1)
    if os.path.exists(input_path2):
        shutil.rmtree(input_path2)
    if os.path.exists(output_path1):
        shutil.rmtree(output_path1)
    if os.path.exists(output_path2):
        shutil.rmtree(output_path2)
    if os.path.exists(output_combined_path):
        shutil.rmtree(output_combined_path)


# Test 3: Two DirectoryStreams with bundle_size=2 and filtering functions
def filter_odd(content: str) -> bool:
    return int(content[-1]) % 2 != 0    # Keep only artifacts with last character as odd number

def filter_even(content: str) -> bool:
    return int(content[-1]) % 2 == 0    # Keep only artifacts with last character as even number

stream_consumer_odd = Consumer(name="Stream1", stream=DirectoryStream(input_path1), bundle_size=2, filter_func=filter_odd)
stream_consumer_even = Consumer(name="Stream2", stream=DirectoryStream(input_path2), bundle_size=2, filter_func=filter_even)
odd_producer = Producer(name="Producer1", directory=output_path1)   # example producer, not used in this test
even_producer = Producer(name="Producer2", directory=output_path2)   # example producer, not used in this test
combined_producer = Producer(name="CombinedProducer", directory=output_combined_path)   # example producer to write combined results, not used in this test

node = Node(name="TestNode", consumers=[stream_consumer_odd, stream_consumer_even], producers=[odd_producer, even_producer, combined_producer])

@node.entrypoint
def node_func():
    for bundle1, bundle2 in zip(stream_consumer_odd, stream_consumer_even):
        with node.stage_run():
            for item1, item2 in zip(bundle1, bundle2):
                odd_producer.write(filename=f"processed_odd_{node.run_id}.txt", content=f"Processed {item1} from odd stream\n")
                even_producer.write(filename=f"processed_even_{node.run_id}.txt", content=f"Processed {item2} from even stream\n")
                combined_producer.write(filename=f"processed_combined_{node.run_id}.txt", content=f"Processed {item1} and {item2} from combined streams\n")


if __name__ == "__main__":

    node.start()

    try:
        node.join()
    except KeyboardInterrupt:
        print(f"Node {node.name} received KeyboardInterrupt. Stopping...")
        node.stop_consumers()

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