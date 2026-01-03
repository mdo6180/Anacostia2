from datetime import datetime
import threading
from queue import Queue
from typing import Dict, List
import time
from logging import Logger
import hashlib

from utils.connection import ConnectionManager
from utils.signal import Signal



class BaseStageNode(threading.Thread):
    def __init__(self, name: str, predecessors: List['BaseStageNode'] = None, logger: Logger = None):
        self.predecessor_queues: Dict[str, Queue] = {}
        self.successor_queues: Dict[str, Queue] = {}
        self.exit_event = threading.Event()
        self.predecessors = predecessors if predecessors is not None else []
        self.logger = logger
        self.conn_manager: ConnectionManager = None

        self.successors = []
        self.predecessors_names = {predecessor.name for predecessor in self.predecessors}
        self.predecessors_names_str = "|".join([predecessor.name for predecessor in self.predecessors])
        self.node_id = f"{name}|{self.predecessors_names_str}"
        self.node_id = hashlib.sha256(self.node_id.encode("utf-8")).hexdigest()

        for predecessor in self.predecessors:
            queue = Queue()
            predecessor.successors.append(self)
            predecessor.set_successor_queue(name, queue)
            self.set_predecessor_queue(predecessor.name, queue)
        
        self.run_id = 0

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

    def initialize_db_connection(self, filename: str):
        self.conn_manager = ConnectionManager(db_path=filename, logger=self.logger)
        latest_run_id = self.conn_manager.get_latest_run_id(node_name=self.name)
        if latest_run_id == 0:
            self.run_id = 0
        else:
            self.run_id = latest_run_id + 1

    def set_predecessor_queue(self, predecessor_name: str, queue: Queue):
        self.predecessor_queues[predecessor_name] = queue
    
    def set_successor_queue(self, successor_name: str, queue: Queue):
        self.successor_queues[successor_name] = queue
    
    def wait_predecessors(self):
        # Wait until all predecessors have sent their signals via the database
        while not self.predecessors_names == self.conn_manager.get_unconsumed_signals(self.name):
            time.sleep(0.1)  # Avoid busy waiting

        for predecessor_name, queue in self.predecessor_queues.items():
            signal: Signal = queue.get()
            self.log(f"{self.name} consumed signal: {predecessor_name} with value {signal}", level="INFO")

            self.conn_manager.consume_signal(
                source_node_name=signal.source_node_name,
                source_run_id=signal.source_run_id,
                target_node_name=self.name,
                target_run_id=self.run_id
            )

    def signal_successors(self):
        for successor in self.successors:
            timestamp = datetime.now()
            signal = Signal(
                source_node_name=self.name, source_run_id=self.run_id, 
                target_node_name=successor.name, target_run_id=successor.run_id, 
                timestamp=timestamp
            )
            successor.predecessor_queues[self.name].put(signal)

            with self.conn_manager.write_cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO run_graph (source_node_name, source_run_id, target_node_name, trigger_timestamp)
                    VALUES (?, ?, ?, ?);
                    """,
                    (self.name, self.run_id, successor.name, timestamp)
                )
            self.log(f"{self.name} signalled {successor.name}", level="INFO")

    def setup(self):
        pass

    def execute(self):
        self.log(f"{self.name} executing", level="INFO")
    
    def exit(self):
        self.conn_manager.close()
    
    def load_signals(self) -> List[Signal]:
        with self.conn_manager.read_cursor() as cursor:
            # Get all distinct source_node_names for the target node
            cursor.execute(
                """
                SELECT source_node_name, source_run_id, trigger_timestamp FROM run_graph WHERE target_node_name = ? AND target_run_id IS NULL;
                """,
                (self.name,)
            )
            rows = cursor.fetchall()

            for row in rows:
                signal = Signal(
                    source_node_name=row[0],
                    source_run_id=row[1],
                    target_node_name=self.name,
                    target_run_id=self.run_id,
                    timestamp=row[2]
                )
                self.predecessor_queues[signal.source_node_name].put(signal)

    def run(self):
        self.load_signals()

        latest_run_id = self.conn_manager.get_latest_run_id(node_name=self.name)

        if latest_run_id == -1:
            self.run_id = 0

        elif self.conn_manager.run_ended(self.name, latest_run_id) is True:
            # upon restart, if the latest run has ended, start a new run
            self.run_id = latest_run_id + 1
        else:
            # upon restart, if the latest run has not ended, resume from that run
            self.run_id = latest_run_id

            self.log(f"{self.name} resuming run {self.run_id}", level="INFO")
            self.conn_manager.resume_run(self.name, self.run_id)
            
            self.execute()
            
            self.log(f"{self.name} finished run {self.run_id}", level="INFO")
            self.conn_manager.end_run(self.name, self.run_id)

            self.signal_successors()

            self.run_id += 1

        while not self.exit_event.is_set():
            if self.exit_event.is_set(): return
            self.wait_predecessors()
            
            if self.exit_event.is_set(): return
            self.conn_manager.start_run(self.name, self.run_id)
            self.log(f"{self.name} starting new run {self.run_id}", level="INFO")

            self.execute()
            
            self.log(f"{self.name} finished run {self.run_id}", level="INFO")
            self.conn_manager.end_run(self.name, self.run_id)
            
            self.signal_successors()

            self.run_id += 1