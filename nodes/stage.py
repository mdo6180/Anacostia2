from contextlib import contextmanager
import threading
from queue import Queue
from typing import Dict, List
import time
from logging import Logger
import sqlite3



class BaseStageNode(threading.Thread):
    def __init__(self, name: str, predecessors: List['BaseStageNode'] = None, logger: Logger = None):
        self.predecessor_queues: Dict[str, Queue] = {}
        self.successor_queues: Dict[str, Queue] = {}
        self.exit_event = threading.Event()
        self.predecessors = predecessors if predecessors is not None else []
        self.logger = logger
        self.conn: sqlite3.Connection = None

        for predecessor in self.predecessors:
            queue = Queue()
            predecessor.set_successor_queue(name, queue)
            self.set_predecessor_queue(predecessor.name, queue)

        self.run_id = 0

        super().__init__(name=name)
    
    def __hash__(self):
        return abs(hash(f"{self.name}"))
    
    @contextmanager
    def read_cursor(self):
        """
        Read-only cursor.
        No commit, no rollback.
        """
        cur = self.conn.cursor()
        try:
            yield cur
        finally:
            cur.close()

    @contextmanager
    def write_cursor(self):
        """
        Write cursor.
        Commits on success, rolls back on error.
        """
        cur = self.conn.cursor()
        try:
            yield cur
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()
            
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

    def initialize_db_connection(self, filename: str):
        self.conn = sqlite3.connect(filename, check_same_thread=False, detect_types=sqlite3.PARSE_DECLTYPES)
        self.conn.execute("PRAGMA journal_mode=WAL;")

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
            self.log(f"{self.name} consumed signal: {signal_name} with value {signal}", level="INFO")
    
    def signal_successors(self):
        for signal_name, queue in self.successor_queues.items():
            queue.put(f"Signal from {self.name}")
            self.log(f"{self.name} produced signal: {signal_name}", level="INFO")
    
    def setup(self):
        pass

    def execute(self):
        self.log(f"{self.name} executing", level="INFO")
    
    def exit(self):
        self.conn.close()

    def run(self):
        while not self.exit_event.is_set():
            if self.exit_event.is_set(): return
            self.wait_predecessors()
            
            if self.exit_event.is_set(): return
            self.execute()
            self.run_id += 1
            
            if self.exit_event.is_set(): return
            self.signal_successors()