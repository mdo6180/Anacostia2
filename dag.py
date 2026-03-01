from typing import List
from logging import Logger
import os

from node import Node
from utils.connection import ConnectionManager

sql = str   # alias of the str type for syntax highlighting using the Python Inline Source Syntax Highlighting extension by Sam Willis in VSCode.



class Graph:
    def __init__(self, name: str, nodes: List[Node], db_folder: str = ".anacostia", logger: Logger = None) -> None:
        self.name = name
        self.nodes = nodes
        self.db_folder = db_folder
        self.logger = logger

        if not os.path.exists(self.db_folder):
            os.makedirs(self.db_folder)
        
        db_path = os.path.join(self.db_folder, 'anacostia.db')
        if os.path.exists(db_path) is True:
            self.log(f"Database found at {db_path}. Connecting...", level="INFO")

        self.conn_manager = ConnectionManager(db_path, logger=self.logger)
        with self.conn_manager.write_cursor() as cursor:
            query: sql = f"""
                CREATE TABLE IF NOT EXISTS nodes (
                    node_name TEXT UNIQUE,
                    node_type TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
            """
            cursor.execute(query)

            query: sql = f"""
                CREATE TABLE IF NOT EXISTS artifact_usage_events (
                    artifact_hash TEXT,
                    node_name TEXT,
                    run_id INTEGER DEFAULT NULL,
                    state TEXT CHECK (state IN ('created', 'committed', 'detected', 'primed', 'using', 'used', 'ignored', 'sent', 'received')),
                    details TEXT DEFAULT NULL,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                );
            """
            cursor.execute(query)

            query: sql = f"""
                CREATE TABLE IF NOT EXISTS provenance_graph (
                    predecessor_hash TEXT DEFAULT NULL,
                    predecessor_type TEXT CHECK (predecessor_type IN ( 'artifact', 'node' )) DEFAULT NULL,
                    successor_hash TEXT,
                    successor_type TEXT CHECK (successor_type IN ( 'artifact', 'node' )),
                    run_id INTEGER,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    details TEXT DEFAULT NULL
                );
                """
            cursor.execute(query)

            query: sql = f"""
                CREATE TABLE IF NOT EXISTS run_events (
                    node_name TEXT,
                    run_id INTEGER,
                    timestamp DATETIME,
                    event_type TEXT NOT NULL CHECK (event_type IN ('start', 'end', 'error', 'restart')),
                    PRIMARY KEY (node_name, run_id, event_type)
                );
            """
            cursor.execute(query)
        
        for node in self.nodes:
            # initialize DB connection for each node, its consumers, and producers
            node.set_db_path(db_path)
            node.setup()

            for consumer in node.consumers:
                consumer.set_db_path(db_path)
                consumer.stream.initialize_db_connection(db_path)
                consumer.stream.setup()
                
            for producer in node.producers:
                producer.initialize_db_connection(db_path)
                producer.setup()

                for transport in producer.transports:
                    transport.initialize_db_connection(db_path)
    
    def start(self):
        self.log(f"Starting graph '{self.name}' with {len(self.nodes)} nodes.", level="INFO")
        for node in self.nodes:
            node.start()
    
    def join(self):
        for node in self.nodes:
            node.join()
    
    def stop(self):
        self.log(f"Stopping graph '{self.name}' with {len(self.nodes)} nodes.", level="INFO")
        for node in self.nodes:
            node.stop_consumers()

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