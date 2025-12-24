import threading
from queue import Queue
from typing import Dict, List
import time
from logging import Logger
from sqlite3 import Cursor



class BaseStageNode(threading.Thread):
    def __init__(self, name: str, predecessors: List['BaseStageNode'] = None, logger: Logger = None):
        self.predecessor_queues: Dict[str, Queue] = {}
        self.successor_queues: Dict[str, Queue] = {}
        self.exit_event = threading.Event()
        self.predecessors = predecessors if predecessors is not None else []
        self.logger = logger
        self.cursor: Cursor = None

        for predecessor in self.predecessors:
            queue = Queue()
            predecessor.set_successor_queue(name, queue)
            self.set_predecessor_queue(predecessor.name, queue)

        super().__init__(name=name)
    
    def log(self, message: str, level="DEBUG") -> None:
        if self.logger is not None:
            if level == "DEBUG":
                self.logger.debug(message)
            elif level == "INFO":
                self.logger.info(message)
            elif level == "WARNING":
                self.logger.warning(message)
            elif level == "ERROR":
                self.logger.error(message)
            elif level == "CRITICAL":
                self.logger.critical(message)
            else:
                raise ValueError(f"Invalid log level: {level}")
        else:
            print(message)

    def set_db_cursor(self, cursor: Cursor):
        self.cursor = cursor

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
            if self.exit_event.is_set(): return
            self.wait_predecessors()
            
            if self.exit_event.is_set(): return
            self.execute()
            
            if self.exit_event.is_set(): return
            self.signal_successors()