from typing import List, Union
from logging import Logger
import os
import sqlite3

from nodes.stage import BaseStageNode
from nodes.watcher import BaseWatcherNode


class Pipeline:
    def __init__(self, name: str, nodes: List[Union[BaseStageNode, BaseWatcherNode]], db_folder: str = ".anacostia", logger: Logger = None) -> None:
        self.name = name
        self.nodes = nodes
        self.db_folder = db_folder
        self.logger = logger

        if not os.path.exists(self.db_folder):
            os.makedirs(self.db_folder)
        
        db_path = os.path.join(self.db_folder, 'anacostia.db')
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        self.cursor = self.conn.cursor()
        #self.cursor.execute('PRAGMA journal_mode=DELETE')
    
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

    def launch_nodes(self):
        self.log(f"Launching nodes for pipeline: {self.name}", level="INFO")
        for node in self.nodes:
            node.start()
        self.log("All nodes launched", level="INFO")
    
    def terminate_nodes(self) -> None:
        self.log("Terminating nodes", level="INFO")
        for node in self.nodes:
            node.exit()
            node.join()
        self.log("All nodes terminated", level="INFO")