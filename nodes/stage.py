import threading
from queue import Queue
from typing import Dict, List
import time



class BaseStageNode(threading.Thread):
    def __init__(self, name: str, predecessors: List['BaseStageNode'] = None):
        self.predecessor_queues: Dict[str, Queue] = {}
        self.successor_queues: Dict[str, Queue] = {}
        self.exit_event = threading.Event()

        super().__init__(name=name)
    
    def set_predecessor_queue(self, predecessor_name: str, queue: Queue):
        self.predecessor_queues[predecessor_name] = queue
    
    def set_successor_queue(self, successor_name: str, queue: Queue):
        self.successor_queues[successor_name] = queue
    
    def wait_predecessors(self):
        # Wait until all predecessor queues have at least one item
        while any(q.empty() for q in self.predecessor_queues.values()):
            time.sleep(0.1)  # Avoid busy waiting

        for signal_name, queue in self.predecessor_queues.items():
            signal = queue.get()
            print(f"{self.name} consumed signal: {signal_name} with value {signal}")
    
    def signal_successors(self):
        for signal_name, queue in self.successor_queues.items():
            queue.put(f"Signal from {self.name}")
            print(f"{self.name} produced signal: {signal_name}")
    
    def execute(self):
        print(f"{self.name} executing")
    
    def run(self):
        while not self.exit_event.is_set():
            self.wait_predecessors()
            self.execute()
            self.signal_successors()