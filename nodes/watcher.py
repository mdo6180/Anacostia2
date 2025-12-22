import threading
from queue import Queue
from typing import Dict
from abc import ABC, abstractmethod
from logging import Logger



class BaseWatcherNode(threading.Thread, ABC):
    def __init__(self, name: str, logger: Logger = None):
        self.successor_queues: Dict[str, Queue] = {}
        self.exit_event = threading.Event()
        self.resource_event = threading.Event()
        self.logger = logger
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
    
    def exit(self):
        self.stop_monitoring()
        self.resource_event.set()
    
    @abstractmethod
    def start_monitoring(self) -> None:
        """
        Override to specify how the resource is monitored. 
        Typically, this method will be used to start an observer that runs in a child thread spawned by the thread running the node.
        """
        pass
    
    @abstractmethod
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