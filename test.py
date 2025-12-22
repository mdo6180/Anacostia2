from nodes.stage import BaseStageNode
from nodes.watcher import BaseWatcherNode



if __name__ == "__main__":
    # Example usage of BaseStageNode and BaseWatcherNode
    watcher_node = BaseWatcherNode(name="WatcherNode1")
    watcher_node2 = BaseWatcherNode(name="WatcherNode2")
    stage_node = BaseStageNode(name="StageNode1", predecessors=[watcher_node, watcher_node2])
    stage_node2 = BaseStageNode(name="StageNode2", predecessors=[stage_node])

    # Start the nodes
    stage_node.start()
    watcher_node.start()

    # Join the nodes (wait for them to finish)
    stage_node.join()
    watcher_node.join()