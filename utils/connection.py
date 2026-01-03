import sqlite3
from contextlib import contextmanager
import logging
from typing import List


class ConnectionManager:
    def __init__(self, db_path: str, logger: logging.Logger = None) -> None:
        self.connection = sqlite3.connect(db_path, check_same_thread=False, detect_types=sqlite3.PARSE_DECLTYPES)
        self.connection.execute("PRAGMA journal_mode=WAL;")
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
    
    def get_latest_run_id(self, node_name: str) -> int:
        with self.read_cursor() as cursor:
            cursor.execute(
                """
                SELECT MAX(run_id) FROM run_events WHERE node_name = ?;
                """,
                (node_name,)
            )
            result = cursor.fetchone()
            if result and result[0] is not None:
                # if there is at least one run, return the latest run_id
                return result[0]
            else:
                # no runs found, return -1
                return -1
    
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
    
    def get_unconsumed_signals(self, target_node_name: str) -> tuple:
        with self.read_cursor() as cursor:
            # Get all distinct source_node_names for the target node
            cursor.execute(
                """
                SELECT DISTINCT source_node_name FROM run_graph WHERE target_node_name = ? AND target_run_id IS NULL;
                """,
                (target_node_name,)
            )
            source_nodes = {row[0] for row in cursor.fetchall()}
            return source_nodes
    
    def get_unconsumed_signals_details(self, target_node_name: str) -> List[tuple]:
        with self.read_cursor() as cursor:
            # Get all distinct source_node_names for the target node
            cursor.execute(
                """
                SELECT source_node_name, source_run_id, trigger_timestamp FROM run_graph WHERE target_node_name = ? AND target_run_id IS NULL;
                """,
                (target_node_name,)
            )
            rows = cursor.fetchall()
            return rows
    
    def consume_signal(self, source_node_name: str, source_run_id: int, target_node_name: str, target_run_id: int) -> None:
        """
        Consumes a signal by updating the target_run_id for a pending edge in the run graph.

        This method finds the oldest pending edge (where target_run_id is NULL) that matches
        the given source node, source run, and target node, then sets its target_run_id to
        complete the connection.

        Args:
            source_node_name (str): The name of the source node that emitted the signal.
            source_run_id (int): The run ID of the source node execution.
            target_node_name (str): The name of the target node that should consume the signal.
            target_run_id (int): The run ID of the target node execution consuming the signal.

        Returns:
            None

        Note:
            Uses SQLite's rowid (an implicit column that exists for every table unless specified
            as WITHOUT ROWID) to efficiently update a specific row. The rowid does not need to
            be explicitly created in the CREATE TABLE schema - it's automatically available in
            SQLite tables.
        """

        with self.write_cursor() as cursor:
            cursor.execute(
                """
                UPDATE run_graph SET target_run_id = ? 
                WHERE rowid = (
                    SELECT rowid FROM run_graph 
                    WHERE source_node_name = ? AND source_run_id = ? AND target_node_name = ? AND target_run_id IS NULL 
                    ORDER BY trigger_timestamp ASC 
                    LIMIT 1
                );
                """,
                (target_run_id, source_node_name, source_run_id, target_node_name,)
            )
                