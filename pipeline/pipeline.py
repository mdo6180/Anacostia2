from contextlib import contextmanager
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
        self.conn = sqlite3.connect(db_path, check_same_thread=False, detect_types=sqlite3.PARSE_DECLTYPES)
        self.conn.execute("PRAGMA journal_mode=WAL;")
        with self.write_cursor() as cursor:
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
                """
                CREATE TABLE IF NOT EXISTS events (
                    event_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    node_id TEXT,
                    event_type TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                );
                """
            )
            cursor.execute(
                f"""
                CREATE TABLE IF NOT EXISTS artifact_usage_events (
                    artifact_path TEXT,
                    hash TEXT,
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
                    node_id TEXT,
                    run_id INTEGER,
                    usage_type TEXT
                );
                """
            )

        for node in self.nodes:
            node.initialize_db_connection(db_path)
            node.setup()

            with self.write_cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO nodes (node_id, node_name, node_type)
                    VALUES (?, ?, ?);
                    """,
                    (hash(node), node.name, type(node).__name__)
                )
    
    @contextmanager
    def read_cursor(self):
        """
        Read-only cursor.
        No commit, no rollback.
        """
        cur = self.conn.cursor()
        try:
            yield cur
        finally:
            cur.close()

    @contextmanager
    def write_cursor(self):
        """
        Write cursor.
        Commits on success, rolls back on error.
        """
        cur = self.conn.cursor()
        try:
            yield cur
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        finally:
            cur.close()
            
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