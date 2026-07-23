import logging
import os
import argparse
import shutil
import time
from pathlib import Path

from anacostia.streams.filesystem import DirectoryStream
from anacostia.consumer import Consumer
from anacostia.producer import Producer
from anacostia.transports.local import FileSystemTransport
from anacostia.node import Node
from anacostia.dag import Graph
from anacostia.utils.debug import stop_if, attach_debugger
from anacostia.utils.types import Artifact


print("imports successful, starting test...")

tests_path = Path("./testing_artifacts")
db_folder_path = tests_path / ".anacostia"
input_path1 = tests_path / "incoming1"
input_path2 = tests_path / "incoming2"
output_path1 = tests_path / "processed1"
output_path2 = tests_path / "processed2"
output_combined_path = tests_path / "processed_combined"
transport_package_dir = tests_path / "transport_dir"
pipeline2_receiver = tests_path / "transport_receiver"

parser = argparse.ArgumentParser(description="Run the pipeline after restart test")
parser.add_argument("-r", "--restart", action="store_true", help="Flag to indicate if this is a restart")
parser.add_argument("-d", "--debug", action="store_true", help="Flag to indicate if debugging is enabled")
args = parser.parse_args()

if args.restart == False:
    if tests_path.exists() is True:
        shutil.rmtree(tests_path)
    tests_path.mkdir(parents=True, exist_ok=True)

if args.debug:
    # To debug this test:
    # Add a breakpoint by clicking on the left side of the line number you want to break on.
    # run the script: python mid_stream_stop.py -r -d
    # open the debug tab in vscode
    # select the "Python Debugger: Remote Attach" configuration, then click on the play button.
    # The script will pause at the breakpoint and you can inspect the values of variables in the debug console.
    attach_debugger()


log_path = tests_path / "anacostia.log"
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    filename=str(log_path),
    filemode='a'
)
logger = logging.getLogger(__name__)


# Test 3: Two DirectoryStreams with bundle_size=2 and filtering functions
def filter_odd(artifact: Artifact) -> bool:
    with open(artifact.location["path"], "r") as f:
        content = f.read()
        return int(content[-1]) % 2 != 0    # Keep only artifacts with last character as odd number

def filter_even(artifact: Artifact) -> bool:
    with open(artifact.location["path"], "r") as f:
        content = f.read()
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

    # All code that comes before this for loop will be executed only on pipeline start and restart, but not on subsequent runs. 

    for bundle1, bundle2 in zip(stream_consumer_odd, stream_consumer_even):
        
        # All code here will execute prior to the run starting
        
        with node.stage_run():

            # All code here will execute during the run

            odd_path = odd_producer.get_staging_directory() / f"processed_odd_{node.run_id}.txt"
            even_path = even_producer.get_staging_directory() / f"processed_even_{node.run_id}.txt"
            
            subdir = combined_producer.get_staging_directory() / f"combined_dir_{node.run_id}"
            os.makedirs(subdir, exist_ok=True)
            combined_staging_path = subdir / f"processed_combined_{node.run_id}.txt"

            for i, (artifact1, artifact2) in enumerate(zip(bundle1, bundle2)):
                # All code here will execute on each item in the bundle

                with open(artifact1.location["path"], "r") as f:
                    content = f.read()
                    with open(odd_path, "a") as file:
                        file.write(f"Processed {artifact1.location['path']} from odd stream with content '{content}'\n")
                
                #time.sleep(1)   # checkpoint 1
                
                with open(artifact2.location["path"], "r") as f:
                    content = f.read()
                    with open(even_path, "a") as file:
                        file.write(f"Processed {artifact2.location['path']} from even stream with content '{content}'\n")
                
                time.sleep(1)   # checkpoint 2
                
                # simulate failure at run 1, iter 0 (first iteration of the second run)
                # disable this stop_if after restart to allow the pipeline to continue and finish processing
                if args.restart == False:
                    stop_if(current_run=node.run_id, current_iter=i, target_run=1, target_iter=0, mode="sigint", logger=logger) 
                
                with open(combined_staging_path, "a") as file:
                    with open(artifact1.location["path"], "r") as f1, open(artifact2.location["path"], "r") as f2:
                        content1 = f1.read()
                        content2 = f2.read()
                        file.write(
                            f"Processed {artifact1.location['path']} with content '{content1}' and {artifact2.location['path']} with content '{content2}' from combined streams\n"
                        )
                
            # All code here will execute before the run ends but after you are done using the bundle

            odd_producer.commit_artifact(
                artifact_staging_path=odd_path, 
                artifact_final_path=odd_producer.get_final_directory() / f"processed_odd_{node.run_id}.txt"
            )

            even_producer.commit_artifact(
                artifact_staging_path=even_path, 
                artifact_final_path=even_producer.get_final_directory() / f"processed_even_{node.run_id}.txt"
            )

            # commit artifacts you want to keep track of. uncommited artifacts will be deleted when run ends. 
            committed_artifact = combined_producer.commit_artifact(
                artifact_staging_path=combined_staging_path, 
                artifact_final_path=combined_producer.get_final_directory() / f"processed_combined_{node.run_id}.txt"
            )

            # stage artifact for prepare for transport packaging
            combined_transport.stage_artifact(
                artifact=committed_artifact,
                artifact_staging_path=combined_transport.get_staging_directory() / f"processed_combined_{node.run_id}.txt",
                producer_name=combined_producer.name
            )

            # package the artifact to create transport package
            package_artifact = combined_transport.package()

            # optional: use the transport to send the artifact to another pipeline
            # you can also skip this step if you are manually transporting artifact over air gap.
            combined_transport.send(
                package_artifact=package_artifact,
                dest_directory=pipeline2_receiver
            )
        
        # All code here outside the node.stage_run() context manager will execute after the run ends but before the next run begins


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