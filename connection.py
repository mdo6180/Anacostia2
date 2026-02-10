import sqlite3
from contextlib import contextmanager
import logging
from typing import List



class ConnectionManager:
    def __init__(self, db_path: str, logger: logging.Logger = None) -> None:
        self.connection = sqlite3.connect(
            db_path, 
            check_same_thread=False, 
            timeout=5.0,                            # wait up to 5 seconds for lock
            isolation_level=None,                   # autocommit mode
            detect_types=sqlite3.PARSE_DECLTYPES
        )
        self.connection.execute("PRAGMA journal_mode=WAL;")
        self.connection.execute("PRAGMA synchronous=NORMAL;")
        self.connection.execute("PRAGMA busy_timeout=5000;")

        self.logger = logger
    
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

    def close(self) -> None:
        self.connection.close()
    
    @contextmanager
    def read_cursor(self):
        """
        Read-only cursor. No commit, no rollback.
        """
        cur = self.connection.cursor()
        try:
            yield cur
        finally:
            cur.close()

    @contextmanager
    def write_cursor(self):
        """
        Write cursor. Commits on success, rolls back on error.
        """
        cur = self.connection.cursor()
        try:
            yield cur
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise
        finally:
            cur.close()
    
    def get_node_id(self, node_name: str) -> int:
        with self.read_cursor() as cursor:
            cursor.execute(
                """
                SELECT node_id FROM nodes WHERE node_name = ?;
                """,
                (node_name,)
            )
            result = cursor.fetchone()
            if result:
                return result[0]
            else:
                raise ValueError(f"Node with name {node_name} not found in database.")
    
    def start_run(self, node_name: str, run_id: int) -> int:
        node_id = self.get_node_id(node_name)

        try:
            with self.write_cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO run_events (node_name, node_id, run_id, timestamp, event_type)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP, 'start');
                    """,
                    (node_name, node_id, run_id)
                )
                return run_id
    
        except sqlite3.IntegrityError as e:
            # Run already started, ignore
            if "UNIQUE constraint failed" in str(e):
                self.log(f"Run {run_id} for node '{node_name}' already started. Ignoring duplicate start.", level="WARNING")
                return -1

    def end_run(self, node_name: str, run_id: int) -> int:
        node_id = self.get_node_id(node_name)

        with self.write_cursor() as cursor:
            try:
                cursor.execute(
                    """
                    INSERT INTO run_events (node_name, node_id, run_id, timestamp, event_type)
                    VALUES (?, ?, ?, CURRENT_TIMESTAMP, 'end');
                    """,
                    (node_name, node_id, run_id)
                )
                return run_id
            
            except sqlite3.IntegrityError as e:
                if "UNIQUE constraint failed" in str(e):
                    # Run already ended, ignore
                    self.log(f"Run {run_id} for node '{node_name}' already ended. Ignoring duplicate end.", level="WARNING")
                    return -1
    
    def run_ended(self, node_name: str, run_id: int) -> bool:
        with self.read_cursor() as cursor:
            cursor.execute(
                """
                SELECT 1 FROM run_events WHERE node_name = ? AND run_id = ? AND event_type = 'end' LIMIT 1;
                """,
                (node_name, run_id)
            )
            return cursor.fetchone() is not None
    
    def resume_run(self, node_name: str, run_id: int) -> None:
        node_id = self.get_node_id(node_name)

        with self.write_cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO run_events (node_name, node_id, run_id, timestamp, event_type)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, 'restart');
                """,
                (node_name, node_id, run_id)
            )
