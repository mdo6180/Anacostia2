import sqlite3
from contextlib import contextmanager
import logging

sql = str   # alias of the str type for syntax highlighting using the Python Inline Source Syntax Highlighting extension by Sam Willis in VSCode.



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
    
    def start_run(self, node_name: str, run_id: int) -> int:
        try:
            with self.write_cursor() as cursor:
                query: sql = """
                    INSERT INTO run_events 
                    (node_name, run_id, timestamp, event_type)
                    VALUES (?, ?, CURRENT_TIMESTAMP, 'start');
                    """
                cursor.execute(query, (node_name, run_id))
                return run_id
    
        except sqlite3.IntegrityError as e:
            # Run already started, ignore
            if "UNIQUE constraint failed" in str(e):
                self.log(f"Run {run_id} for node '{node_name}' already started. Ignoring duplicate start.", level="WARNING")
                return -1

    def end_run(self, node_name: str, run_id: int) -> int:
        with self.write_cursor() as cursor:
            try:
                query: sql = """
                    INSERT INTO run_events 
                    (node_name, run_id, timestamp, event_type)
                    VALUES (?, ?, CURRENT_TIMESTAMP, 'end');
                """
                cursor.execute(query, (node_name, run_id))
                return run_id
            
            except sqlite3.IntegrityError as e:
                if "UNIQUE constraint failed" in str(e):
                    # Run already ended, ignore
                    self.log(f"Run {run_id} for node '{node_name}' already ended. Ignoring duplicate end.", level="WARNING")
                    return -1
    
    def run_ended(self, node_name: str, run_id: int) -> bool:
        with self.read_cursor() as cursor:
            query: sql = """
                SELECT 1 FROM run_events WHERE node_name = ? AND run_id = ? AND event_type = 'end' LIMIT 1;
            """
            cursor.execute(query, (node_name, run_id))
            return cursor.fetchone() is not None
    
    def resume_run(self, node_name: str, run_id: int) -> None:
        with self.write_cursor() as cursor:
            query: sql = """
                INSERT INTO run_events 
                (node_name, run_id, timestamp, event_type)
                VALUES (?, ?, CURRENT_TIMESTAMP, 'restart');
                """
            cursor.execute(query, (node_name, run_id))

    def get_latest_run_id(self, node_name: str) -> int:
        with self.read_cursor() as cursor:
            query: sql = """
                SELECT MAX(run_id) FROM run_events WHERE node_name = ?;
            """
            cursor.execute(query, (node_name,))
            result = cursor.fetchone()
            if result and result[0] is not None:
                # if there is at least one run, return the latest run_id
                return result[0]
            else:
                # no runs found, return -1
                return -1
    