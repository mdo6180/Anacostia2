from pathlib import Path

from anacostia.streams.directory import DirectoryStream
from anacostia.consumer import Consumer
from anacostia.node import Node
from anacostia.dag import Graph

# 1. Set up streams, consumers, and nodes
tests_path = Path("./testing_artifacts")
db_folder_path = tests_path / ".anacostia"
input_path1 = tests_path / "incoming1"

stream = DirectoryStream(name="odd_folder", directory=input_path1)
stream_consumer_odd = Consumer(name="stream_consumer_odd", stream=stream)
node = Node(name="TestNode", consumers=[stream_consumer_odd])

# 2. Define the node's processing function
@node.entrypoint
def node_func():
    for bundle in stream_consumer_odd:
        with node.stage_run():
            print(f"processed bundle {bundle} in run {node.run_id}")

# 3. Create and start the graph
graph = Graph(name="TestGraph", nodes=[node], db_folder=db_folder_path)

graph.start()
try:
    graph.join()
except KeyboardInterrupt:
    graph.stop()