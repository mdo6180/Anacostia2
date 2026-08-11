from typing import List
from logging import Logger
from pathlib import Path

from anacostia.node import Stage
from anacostia.utils.connection import ConnectionManager
from anacostia.utils.logging import log

sql = str   # alias of the str type for syntax highlighting using the Python Inline Source Syntax Highlighting extension by Sam Willis in VSCode.



class Graph:
    def __init__(self, name: str, nodes: List[Stage], db_folder: Path = ".anacostia", logger: Logger = None) -> None:
        self.name = name
        self.nodes = nodes
        self.db_folder = db_folder
        self.logger = logger

        if not self.db_folder.exists():
            self.db_folder.mkdir(parents=True, exist_ok=True)
        
        db_path = self.db_folder / 'anacostia.db'
        if db_path.exists() is True:
            log(f"Database found at {db_path}. Connecting...", level="info", logger=self.logger)

        self.conn_manager = ConnectionManager(db_path, logger=self.logger)
        self.conn_manager.create_global_tables()

        for node in self.nodes:
            # initialize DB connection for each node, its consumers, and producers
            node.set_db_path(db_path)
            node.initialize_db_connection(db_path)
            node.set_db_folder(self.db_folder)
            node.set_staging_directory(self.db_folder / 'staging')
            node.setup()

            for consumer in node.consumers:
                consumer.set_db_path(db_path)
                consumer.stream.initialize_db_connection(db_path)
                consumer.stream.setup()
                
            for producer in node.producers:
                producer.set_db_folder(self.db_folder)
                producer.initialize_db_connection(db_path)
                producer.setup()

            for transport in node.transports:
                transport.set_db_folder(self.db_folder)
                transport.initialize_staging_directory()
                transport.initialize_db_connection(db_path)
                transport.setup()

    def start(self):
        log(f"Starting graph '{self.name}' with {len(self.nodes)} nodes.", level="info", logger=self.logger)
        for node in self.nodes:
            node.start()
    
    def join(self):
        for node in self.nodes:
            node.join()
    
    def stop(self):
        log(f"Stopping graph '{self.name}' with {len(self.nodes)} nodes.", level="info", logger=self.logger)
        for node in self.nodes:
            node.stop_consumers()
