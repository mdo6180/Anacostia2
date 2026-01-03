from datetime import datetime
import threading
from typing import List
import time
from logging import Logger
import hashlib

from utils.connection import ConnectionManager



class BaseStageNode(threading.Thread):
    def __init__(self, name: str, predecessors: List['BaseStageNode'] = None, logger: Logger = None):
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
            predecessor.successors.append(self)
        
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

    def wait_predecessors(self):
        # Wait until all predecessors have sent their signals via the database

        def _get_unconsumed_signals_names():
            uncomsuned_signals = self.conn_manager.get_unconsumed_signals(self.name)
            uncomsuned_signals_names = set(source_node_name for source_node_name, _, _ in uncomsuned_signals)
            return uncomsuned_signals_names

        while not self.predecessors_names == _get_unconsumed_signals_names():
            time.sleep(0.1)  # Avoid busy waiting

        for predecessor_name in self.predecessors_names:
            self.conn_manager.consume_signal(
                source_node_name=predecessor_name,
                target_node_name=self.name,
                target_run_id=self.run_id
            )
            self.log(f"{self.name} received signal from {predecessor_name}", level="INFO")

    def signal_successors(self):
        for successor in self.successors:
            self.conn_manager.signal_successor(
                source_node_name=self.name,
                source_run_id=self.run_id,
                target_node_name=successor.name
            )
            self.log(f"{self.name} signalled {successor.name}", level="INFO")

    def setup(self):
        pass

    def execute(self):
        self.log(f"{self.name} executing", level="INFO")
    
    def exit(self):
        self.conn_manager.close()
    
    def run(self):
        latest_run_id = self.conn_manager.get_latest_run_id(node_name=self.name)

        if latest_run_id == -1:
            self.run_id = 0

        elif self.conn_manager.run_ended(self.name, latest_run_id) is True:
            # upon restart, if the latest run has ended, start a new run
            self.run_id = latest_run_id + 1
        else:
            # upon restart, if the latest run has not ended, resume from that run
            self.run_id = latest_run_id

            self.log(f"{self.name} restarting run {self.run_id}", level="INFO")
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
            
            # we don't wait for the exit event here to allow successors to be signalled even if we are terminating
            # if the node exits, we want it to exit before the end_run is called, otherwise, we want to log the run into the run graph
            self.signal_successors()

            self.run_id += 1