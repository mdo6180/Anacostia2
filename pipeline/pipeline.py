from typing import List, Union
from logging import Logger
import os

from nodes.stage import BaseStageNode
from nodes.watcher import BaseWatcherNode
from utils.connection import ConnectionManager



class Pipeline:
    def __init__(self, name: str, nodes: List[Union[BaseStageNode, BaseWatcherNode]], db_folder: str = ".anacostia", logger: Logger = None) -> None:
        self.name = name
        self.nodes = nodes
        self.db_folder = db_folder
        self.logger = logger

        if not os.path.exists(self.db_folder):
            os.makedirs(self.db_folder)
        
        db_path = os.path.join(self.db_folder, 'anacostia.db')
        if os.path.exists(db_path) is True:
            self.log(f"Database found at {db_path}. Connecting...", level="INFO")

        self.conn_manager = ConnectionManager(db_path)
        with self.conn_manager.write_cursor() as cursor:
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS nodes (
                    node_id TEXT PRIMARY KEY,
                    node_name TEXT UNIQUE,
                    node_type TEXT,
                    created_at DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS artifact_usage_events (
                    artifact_path TEXT,
                    artifact_hash TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    node_id TEXT,
                    node_name TEXT,
                    run_id INTEGER,
                    state TEXT CHECK (state IN ( 'created', 'accessed', 'committed', 'detected', 'primed', 'using', 'used', 'ignored')),
                    edge_type TEXT CHECK (edge_type IN ('input', 'output')),
                    details TEXT DEFAULT NULL
                );
                """
            )
            # in the future, replace source_node_name and target_node_name with node_id foreign keys
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS run_graph (
                    source_node_name TEXT,
                    source_run_id INTEGER,
                    target_node_name TEXT,
                    target_run_id INTEGER,
                    trigger_timestamp DATETIME
                );
                """
            )
            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS run_events (
                    node_name TEXT,
                    node_id TEXT,
                    run_id INTEGER,
                    timestamp DATETIME,
                    event_type TEXT NOT NULL CHECK (event_type IN ('start', 'end', 'error', 'restart')),
                    PRIMARY KEY (node_id, run_id, event_type)
                );
                """
            )

        for node in self.nodes:
            node.initialize_db_connection(db_path)
            node.setup()

            with self.conn_manager.write_cursor() as cursor:
                cursor.execute(
                    """
                    INSERT OR IGNORE INTO nodes (node_id, node_name, node_type)
                    VALUES (?, ?, ?);
                    """,
                    (node.node_id, node.name, type(node).__name__)
                )

    def log(self, message: str, level="DEBUG", color: str | None = None) -> None:
        ANSI_COLORS = {
            "black": "\033[30m",
            "red": "\033[31m",
            "green": "\033[32m",
            "yellow": "\033[33m",
            "blue": "\033[34m",
            "magenta": "\033[35m",
            "cyan": "\033[36m",
            "white": "\033[37m",
            "reset": "\033[0m",
        }

        if color is not None:
            if color not in ANSI_COLORS:
                raise ValueError(f"Invalid color: {color}")
            message = f"{ANSI_COLORS[color]}{message}{ANSI_COLORS['reset']}"

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
        self.log(f"Launching nodes for pipeline: {self.name}", level="INFO", color="green")
        for node in self.nodes:
            node.start()
        self.log("All nodes launched", level="INFO", color="green")
    
    def terminate_nodes(self) -> None:
        self.log("Terminating nodes", level="INFO", color="yellow")
        for node in self.nodes:
            node.exit()
            node.join()
        self.log("All nodes terminated", level="INFO", color="yellow")