from abc import ABC
from functools import wraps
import inspect
from logging import Logger
import os
import threading
from contextlib import contextmanager
from typing import Callable, List

from utils.connection import ConnectionManager
from consumers.consumer import Consumer
from producers.producer import Producer



class Node(threading.Thread, ABC):
    def __init__(self, name: str, consumers: List[Consumer], producers: List[Producer], logger: Logger = None):
        self.consumers = consumers
        self.producers = producers
        self.run_id = 0
        self.logger = logger
        
        self.stream_consumer_odd = consumers[0]
        self.stream_consumer_even = consumers[1]

        self.odd_producer = producers[0]
        self.even_producer = producers[1]
        self.combined_producer = producers[2]

        self._entrypoint = None

        super().__init__(name=name, daemon=True)
    
    def initialize_db_connection(self, filename: str):
        self.conn_manager = ConnectionManager(db_path=filename, logger=self.logger)
    
    def setup(self):
        pass

    def start_consumers(self):
        # in the future, add logic here to check if there are any primed artifacts that haven't been marked as being used in the DB 
        # and yield those first before starting to consume new artifacts from the stream
        for consumer in self.consumers:
            consumer.start()
    
    def stop_consumers(self):
        for consumer in self.consumers:
            consumer.stop()
    
    def using_artifacts(self):
        # placeholder for logic to mark artifacts as being used in the DB
        hashes = []
        for consumer in self.consumers:
            hashes.extend(consumer.bundle_hashes)
        self.logger.info(f"{self.name} using_artifact: {hashes}")     # using_artifact DB call in future
    
    def commit_artifacts(self):
        # placeholder for logic to mark artifacts as committed in the DB
        hashes = []
        for consumer in self.consumers:
            hashes.extend(consumer.bundle_hashes)
        self.logger.info(f"{self.name} committed_artifact: {hashes}")     # commit_artifact DB call in future

    @contextmanager
    def stage_run(self):
        try:
            self.logger.info(f"\nNode {self.name} starting run {self.run_id}")   # start_run DB call in future
            self.using_artifacts()    # mark artifacts as being used in the DB
            
            yield
            
            self.commit_artifacts()   # mark artifacts as committed in the DB
            self.logger.info(f"Node {self.name} finished run {self.run_id}\n")    # end_run DB call in future
            self.run_id += 1

        except Exception as e:
            self.logger.error(f"Error in node {self.name} during run {self.run_id}: {e}")
            # log the error in DB here, used to display run error on GUI

    def entrypoint(self, func: Callable[[], None]):
        # 🔒 Enforce exactly ONE entrypoint
        if self._entrypoint is not None:
            prev_file = inspect.getsourcefile(self._entrypoint)
            new_file = inspect.getsourcefile(func)

            prev_line = inspect.getsourcelines(self._entrypoint)[1]
            new_line = inspect.getsourcelines(func)[1]

            prev_name = os.path.basename(prev_file) if prev_file else "<unknown>"
            new_name = os.path.basename(new_file) if new_file else "<unknown>"

            raise RuntimeError(
                f"Node '{self.name}' already has entrypoint '{self._entrypoint.__name__}' "
                f"(defined in {prev_name}:{prev_line}). "
                f"Attempted second entrypoint '{func.__name__}' "
                f"(defined in {new_name}:{new_line})."
            )

        self._entrypoint = func

        @wraps(func)
        def wrapper():
            return func()

        return wrapper

    def run(self):
        if self._entrypoint is None:
            raise RuntimeError(f"No entrypoint registered for node {self.name}. Use @node.entrypoint")

        try:
            self.start_consumers()
            # get the max run_id from the DB for this node and set self.run_id to max_run_id + 1 here, so that run IDs are consistent across restarts

            self._entrypoint()

        except Exception as e:
            self.logger.error(f"Error in node {self.name}: {e}")
            # log the error in DB here, used to display run error on GUI