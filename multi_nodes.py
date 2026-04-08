import logging
import os
import argparse
import shutil
import time
from pathlib import Path

from streams.directory import DirectoryStream
from producer import Producer
from consumer import Consumer
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
transport_dest_path = f"{tests_path}/transport_dest"
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

class FolderTransport(FileSystemTransport):
    def __init__(self, name: str, dest_directory: str, dest_stream_name: str, logger: logging.Logger = None):
        super().__init__(name, dest_directory, dest_stream_name, logger)

    def send(self, folderpath: str, artifact_hash: str) -> None:
        foldername = os.path.basename(folderpath)
        dest_path = os.path.join(self.dest_directory, foldername)
        
        # the protocol to actually send the artifact
        shutil.copytree(folderpath, dest_path)

        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
                INSERT OR IGNORE INTO {self.local_table_name} 
                (artifact_src_path, artifact_dest_path, artifact_hash, node_name, hash_algorithm)
                VALUES (?, ?, ?, ?, ?);
            """
            cursor.execute(query, (folderpath, dest_path, artifact_hash, self.name, "sha256"))

# example transport to move artifacts from producers to the transport's destination directory
# Note: make sure the dest_stream_name corresponds to the name of the stream monitoring the destination directory for this transport 
# (in this case, "combined_folder") so that the stream can correctly track the artifacts in its local table and the global usage table in the DB.
file_transport = FolderTransport(name="file_transport", dest_directory=transport_dest_path, dest_stream_name="combined_folder", logger=logger)

# example producer to write combined results and send to transport
combined_producer = Producer(name="combined_producer", directory=output_combined_path, transports=[file_transport], logger=logger)

node = Node(name="TestNode", consumers=[stream_consumer_odd, stream_consumer_even], producers=[odd_producer, even_producer, combined_producer], logger=logger)

@node.entrypoint
def node_func():
    if node.run_id == 0:
        # if it's the first run (i.e. if it's the first time this pipeline has ever been ran), execute the normal entrypoint logic
        logger.info(f"Node {node.name} starting entrypoint function for run {node.run_id}")
    else:
        # if it's a restart, execute custom logic before resuming the entrypoint function
        logger.info(f"Node {node.name} executing custom restart logic for run {node.run_id}")

    # IMPORTANT: .get_staging_directory() must be called after the producer has been initialized with the DB connection in the node's setup() method, 
    # since the staging directory is created inside the DB folder.
    # Thus, best practice is to call .get_staging_directory() inside the node's entrypoint function, 
    # because the producer's setup() method will be called before the entrypoint function.
    odd_staging_path = odd_producer.get_staging_directory()
    even_staging_path = even_producer.get_staging_directory()
    combined_staging_path = combined_producer.get_staging_directory()

    # Note: all code that comes before this for loop will be executed only on pipeline start and restart, but not on subsequent runs. 
    # All code inside this for loop will be executed on every run, including the first run and subsequent runs after restart. 
    # So if you want some code to be executed only on restart but not on the first run, 
    # you can check if node.run_id == 0 to determine if it's the first run or a restart, and execute the code accordingly.
    # Restart logic example: load previously trained model and the state of the optimizer from the model registry and continue training in the current run.
    for bundle1, bundle2 in zip(stream_consumer_odd, stream_consumer_even):
        with node.stage_run():
            for i, (item1, item2) in enumerate(zip(bundle1, bundle2)):
                with open(os.path.join(odd_staging_path, f"processed_odd_{node.run_id}.txt"), "a") as file:
                    file.write(f"Processed {item1} from odd stream\n")
                
                time.sleep(1)   # checkpoint 1
                
                with open(os.path.join(even_staging_path, f"processed_even_{node.run_id}.txt"), "a") as file:
                    file.write(f"Processed {item2} from even stream\n")
                
                time.sleep(1)   # checkpoint 2
                
                """
                # simulate failure at run 1, iter 0 (first iteration of the second run)
                # disable this stop_if after restart to allow the pipeline to continue and finish processing
                if args.restart == False:
                    stop_if(current_run=node.run_id, current_iter=i, target_run=1, target_iter=0, mode="sigint", logger=logger) 
                """
                
                subdir = os.path.join(combined_staging_path, f"combined_dir_{node.run_id}")
                os.makedirs(subdir, exist_ok=True)
                file_path = os.path.join(subdir, f"processed_combined_{node.run_id}.txt")
                with open(file_path, "a") as file:
                    file.write(f"Processed {item1} and {item2} from combined streams\n")
                
                time.sleep(1)   # checkpoint 3


class CombinedStream(DirectoryStream):
    def __init__(self, name: str, directory: str, poll_interval: float = 0.1, hash_chunk_size: int = 1_048_576, logger: logging.Logger = None):
        super().__init__(name, directory, poll_interval, hash_chunk_size, logger)

    def load_artifact(self, artifact_hash):
        folder_path = self.get_artifact_path(artifact_hash)
        folder_path = Path(folder_path)
        for filename in folder_path.iterdir():
            if filename.is_file():
                with open(filename, "r") as file:
                    content = file.read()
                return content


class ModelRetrainingNode(Node):
    def __init__(self, name: str, consumers: list[Consumer], producers: list[Producer], logger: logging.Logger = None):
        super().__init__(name, consumers, producers, logger)
        self.metrics_table_name = f"{self.name}_metrics"

    def setup(self):
        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
                CREATE TABLE IF NOT EXISTS {self.metrics_table_name} (
                    run_id INT,
                    epoch INT,
                    accuracy FLOAT,
                    model_path TEXT,
                    model_hash TEXT
                );
                """
            cursor.execute(query)
    
    def log_metrics(self, epoch: int, accuracy: float, model_path: str):
        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
                INSERT INTO {self.metrics_table_name} (run_id, epoch, accuracy, model_path, model_hash)
                VALUES (?, ?, ?, ?, ?);
                """
            cursor.execute(query, (self.run_id, epoch, accuracy, model_path, None))
    
    def log_model_hash(self, model_path: str, model_hash: str) -> str:
        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
                UPDATE {self.metrics_table_name}
                SET model_hash = ?
                WHERE model_path = ?;
            """
            cursor.execute(query, (model_hash, model_path))
        return model_hash


combined_consumer = Consumer(
    name="combined_consumer", 
    stream=CombinedStream(name="combined_folder", directory=transport_dest_path, logger=logger), 
    logger=logger
)
model_registry_producer = Producer(name="model_registry_producer", directory=model_registry_path, logger=logger)

node2 = ModelRetrainingNode(name="ModelRetrainingNode", consumers=[combined_consumer], producers=[model_registry_producer], logger=logger)

@node2.entrypoint
def node2_func():
    logger.info(f"Node {node2.name} starting entrypoint function for run {node2.run_id}")
    model_registry_staging_path = model_registry_producer.get_staging_directory()

    for bundle in combined_consumer:
        with node2.stage_run():
            for i, item in enumerate(bundle):

                subdir = os.path.join(model_registry_staging_path, f"model_dir_{node2.run_id}")
                os.makedirs(subdir, exist_ok=False)
                model_path = os.path.join(subdir, f"model_{node2.run_id}.txt")

                with open(model_path, "a") as file:
                    file.write(f"Model retrained with:\n")

                logger.info(f"ModelRetrainingNode writing: 'Model retrained with:' to {model_path} in run {node2.run_id}")
                time.sleep(1)   # checkpoint

                # simulate failure at run 1, iter 0 (first iteration of the second run)
                # disable this stop_if after restart to allow the pipeline to continue and finish processing
                if args.restart == False:
                    stop_if(current_run=node2.run_id, current_iter=i, target_run=1, target_iter=0, mode="sigint", logger=logger) 
                    #model_registry_producer.create_artifact(filename=model_name, content=f"restarting\n")

                with open(model_path, "a") as file:
                    file.write(f"{item}\n")
                logger.info(f"ModelRetrainingNode writing: '{item}' to {model_path} in run {node2.run_id}")
                time.sleep(1)   # checkpoint

                node2.log_metrics(epoch=i, accuracy=0.8 + i*0.01, model_path=model_path)   # example metrics logging

        # user can log metadata here after the run is done if they need the artifacts' hashes because hashes are only computed at the end of the run. 
        # this is done by design because hashing large artifacts is quite time consuming; so it's better to finish the work for the run, then hash.
        # but if the user does not need the artifacts' hashes, then they can log metadata during the run.  

                """ 
        # simulate failure at run 1, iter 0 (first iteration of the second run)
        # disable this stop_if after restart to allow the pipeline to continue and finish processing
        if args.restart == False:
            stop_if(current_run=node2.run_id, current_iter=i, target_run=1, target_iter=0, mode="sigint", logger=logger) 
            #model_registry_producer.create_artifact(filename=model_name, content=f"restarting\n")
                """ 

graph = Graph(name="TestGraph", nodes=[node, node2], db_folder=db_folder_path, logger=logger)


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