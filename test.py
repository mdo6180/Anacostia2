from queue import Queue
from nodes.stage import BaseStageNode
from nodes.watcher import BaseWatcherNode

if __name__ == "__main__":
    # Example usage of BaseStageNode and BaseWatcherNode
    stage_node = BaseStageNode(name="StageNode1")
    watcher_node = BaseWatcherNode(name="WatcherNode1")

    # Start the nodes
    stage_node.start()
    watcher_node.start()

    # Join the nodes (wait for them to finish)
    stage_node.join()
    watcher_node.join()