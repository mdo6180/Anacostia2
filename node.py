from abc import ABC
from functools import wraps
import inspect
from logging import Logger
import os
import threading
from contextlib import contextmanager
from typing import Callable, List
import traceback

from utils.connection import ConnectionManager
from consumer import Consumer
from producers.producer import Producer

sql = str   # alias of the str type for syntax highlighting using the Python Inline Source Syntax Highlighting extension by Sam Willis in VSCode.



class Node(threading.Thread, ABC):
    def __init__(self, name: str, consumers: List[Consumer], producers: List[Producer], logger: Logger = None):
        self.consumers = consumers
        self.producers = producers
        self.run_id = 0
        self.logger = logger
        
        self._entrypoint = None
        self._restart = None
        
        self.global_usage_table_name = "artifact_usage_events"

        super().__init__(name=name, daemon=True)
    
    def set_db_path(self, db_path: str):
        self.db_path = db_path

    def setup(self):
        pass

    def start_consumers(self):
        for consumer in self.consumers:
            consumer.set_node_name(self.name)

        # in the future, add logic here to check if there are any primed artifacts that haven't been marked as being used in the DB 
        # and yield those first before starting to consume new artifacts from the stream
        for consumer in self.consumers:
            consumer.start()
    
    def stop_consumers(self):
        for consumer in self.consumers:
            consumer.stop()
    
    def set_run_id(self, run_id: int):
        self.run_id = run_id

        for consumer in self.consumers:
            consumer.set_run_id(run_id)
        
        for producer in self.producers:
            producer.set_run_id(run_id)

    def start_using_artifact(self, filepath: str, artifact_hash: str) -> None:
        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} 
                (artifact_hash, node_name, run_id, state, details) 
                VALUES (?, ?, ?, ?, ?);
            """
            cursor.execute(query, (artifact_hash, self.name, self.run_id, "using", filepath))
    
    def finished_using_artifact(self, filepath: str, artifact_hash: str) -> None:
        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
                INSERT OR IGNORE INTO {self.global_usage_table_name} 
                (artifact_hash, node_name, run_id, state, details) 
                VALUES (?, ?, ?, ?, ?);
            """
            cursor.execute(query, (artifact_hash, self.name, self.run_id, "used", filepath))
    
    def using_artifacts(self):
        for consumer in self.consumers:
            for artifact_hash in consumer.bundle_hashes:
                artifact_path = consumer.stream.get_artifact_path(artifact_hash)
                self.start_using_artifact(artifact_path, artifact_hash)
                self.logger.info(f"Node {self.name} started using artifact {artifact_path} with hash {artifact_hash} in run {self.run_id}")
    
    def commit_artifacts(self):
        for consumer in self.consumers:
            for artifact_hash in consumer.bundle_hashes:
                artifact_path = consumer.stream.get_artifact_path(artifact_hash)
                self.finished_using_artifact(artifact_path, artifact_hash)
                self.logger.info(f"Node {self.name} committed artifact {artifact_path} with hash {artifact_hash} in run {self.run_id}")
        
        for producer in self.producers:
            producer.register_created_artifacts()   # register all created artifacts in the DB
            producer.paths_in_current_run.clear()   # clear the set for the next run

    @contextmanager
    def stage_run(self):
        try:
            self.logger.info(f"\nNode {self.name} starting run {self.run_id}")   # start_run DB call in future
            self.conn_manager.start_run(self.name, self.run_id)   # start_run DB call
            self.set_run_id(self.run_id)
            self.using_artifacts()    # mark artifacts as being used in the DB
            
            yield
            
            self.commit_artifacts()   # mark artifacts as committed in the DB
            self.logger.info(f"\nNode {self.name} finished run {self.run_id}")    # end_run DB call in future
            self.conn_manager.end_run(self.name, self.run_id)   # end_run DB call
            self.set_run_id(self.run_id + 1)  # prepare for next run

        except Exception as e:
            self.logger.error(f"Error in node {self.name} during run {self.run_id}: {e}")
            # log the error in DB here, used to display run error on GUI

    def restart(self, func: Callable[[], None]):
        # 🔒 Enforce exactly ONE restart
        if self._restart is not None:
            prev_file = inspect.getsourcefile(self._restart)
            new_file = inspect.getsourcefile(func)

            prev_line = inspect.getsourcelines(self._restart)[1]
            new_line = inspect.getsourcelines(func)[1]

            prev_name = os.path.basename(prev_file) if prev_file else "<unknown>"
            new_name = os.path.basename(new_file) if new_file else "<unknown>"

            raise RuntimeError(
                f"Node '{self.name}' already has restart '{self._restart.__name__}' "
                f"(defined in {prev_name}:{prev_line}). "
                f"Attempted second restart '{func.__name__}' "
                f"(defined in {new_name}:{new_line})."
            )

        self._restart = func

        @wraps(func)
        def wrapper():
            return func()

        return wrapper

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
        self.conn_manager = ConnectionManager(db_path=self.db_path, logger=self.logger)

        latest_run_id = self.conn_manager.get_latest_run_id(node_name=self.name)

        if latest_run_id == -1:
            self.set_run_id(0)

        elif self.conn_manager.run_ended(self.name, latest_run_id) is True:
            # upon restart, if the latest run has ended, start a new run
            self.set_run_id(latest_run_id + 1)

            for consumer in self.consumers:
                # restart mode 1 means the latest run ended 
                consumer.set_restart_mode(mode=1)   

            if self._restart is not None:
                self._restart()
        
        else:
            # upon restart, if the latest run has not ended, resume from that run
            self.set_run_id(latest_run_id)
            self.logger.info(f"{self.name} restarting run {self.run_id}")
            self.conn_manager.resume_run(self.name, self.run_id)

            for consumer in self.consumers:
                # restart mode 2 means the latest run did not end
                consumer.set_restart_mode(mode=2)

            if self._restart is not None:
                self._restart()
        
        if self._entrypoint is None:
            raise RuntimeError(f"No entrypoint registered for node {self.name}. Use @node.entrypoint")

        try:
            self.start_consumers()
            # get the max run_id from the DB for this node and set self.run_id to max_run_id + 1 here, so that run IDs are consistent across restarts

            self._entrypoint()

        except Exception as e:
            self.logger.error(f"Error in node {self.name}: {e}\n{traceback.format_exc()}")
            # log the error in DB here, used to display run error on GUI