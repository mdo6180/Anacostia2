import threading
from queue import Queue
from typing import Dict
from abc import ABC, abstractmethod
from logging import Logger
import os
import time
import traceback
from threading import Thread
from sqlite3 import Cursor
from datetime import datetime

from nodes.utils import EventType



class BaseWatcherNode(threading.Thread, ABC):
    def __init__(self, name: str, path: str, logger: Logger = None):
        self.path = path
        if os.path.exists(self.path) is False:
            os.makedirs(self.path)

        self.successor_queues: Dict[str, Queue] = {}
        self.exit_event = threading.Event()
        self.resource_event = threading.Event()
        self.logger = logger
        self.cursor: Cursor = None
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

    def set_successor_queue(self, successor_name: str, queue: Queue):
        self.successor_queues[successor_name] = queue
    
    def set_db_cursor(self, cursor: Cursor):
        self.cursor = cursor

    def exit(self):
        self.stop_monitoring()
        self.resource_event.set()
    
    def start_monitoring(self) -> None:
        """
        Override to specify how the resource is monitored. 
        Typically, this method will be used to start an observer that runs in a child thread spawned by the thread running the node.
        """

        def _monitor_thread_func():
            self.log(f"Starting observer thread for node '{self.name}'", level="INFO")
            while self.exit_event.is_set() is False:
                for root, dirnames, filenames in os.walk(self.path):
                    for filename in filenames:
                        filepath = os.path.join(root, filename)
                        
                        try:
                            self.log(f"detected file {filepath}", level="INFO")
                            '''
                            self.cursor.execute(
                                "INSERT INTO events (node_id, event_type, timestamp) VALUES (?, ?, ?);",
                                (self.name, EventType.FILE_DETECTED, datetime.now())
                            )
                            '''
                        
                        except Exception as e:
                            self.log(f"Unexpected error in monitoring logic for '{self.name}': {traceback.format_exc()}", level="ERROR")

                if self.exit_event.is_set() is True: 
                    self.log(f"Observer thread for node '{self.name}' exiting", level="INFO")
                    return
                try:
                    self.trigger()
                
                except Exception as e:
                    self.log(f"Error checking resource in node '{self.name}': {traceback.format_exc()}", level="ERROR")

                # sleep for a while before checking again
                time.sleep(0.1)

            self.log(f"Observer thread for node '{self.name}' exited", level="INFO")

        # since we are using asyncio.run, we need to create a new thread to run the event loop 
        # because we can't run an event loop in the same thread as the FilesystemStoreNode
        self.observer_thread = Thread(name=f"{self.name}_observer", target=_monitor_thread_func, daemon=True)
        self.observer_thread.start()
    
    def stop_monitoring(self) -> None:
        """
        Override to specify how the resource is monitored. 
        Typically, this method will be used to start an observer that runs in a child thread spawned by the thread running the node.
        """
        pass

    def trigger(self, message: str = None) -> None:
        if self.resource_event.is_set() is False:
            self.resource_event.set()
            print(f"{self.name} triggered with message: {message}")
    
    def signal_successors(self):
        for signal_name, queue in self.successor_queues.items():
            queue.put(f"Signal from {self.name}")
            print(f"{self.name} produced signal: {signal_name}")
    
    def execute(self):
        print(f"{self.name} executing")
    
    def run(self):
        self.start_monitoring()     # Start monitoring the resource in a separate thread

        while not self.exit_event.is_set():

            if self.exit_event.is_set(): return
            self.resource_event.wait()      # Wait until the resource event is set

            if self.exit_event.is_set(): return
            self.execute()

            if self.exit_event.is_set(): return
            self.signal_successors()

            if self.exit_event.is_set(): return
            self.resource_event.clear()     # Reset the event for the next cycle