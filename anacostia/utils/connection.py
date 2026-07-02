import sqlite3
from contextlib import contextmanager
import logging

from anacostia.utils.logging import log

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

        self.db_path = db_path
        self.logger = logger
    
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
    
    def create_global_tables(self) -> None:
        with self.write_cursor() as cursor:
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
                    state TEXT CHECK (state IN ('created', 'committed', 'detected', 'primed', 'using', 'used', 'ignored', 'sent', 'received', 'packaged')),
                    details TEXT DEFAULT NULL CHECK (details IS NULL OR json_valid(details)),
                    timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
                );
            """
            cursor.execute(query)

            query: sql = f"""
                CREATE TABLE IF NOT EXISTS provenance_graph (
                    predecessor_name TEXT DEFAULT NULL,
                    predecessor_type TEXT DEFAULT NULL,
                    successor_name TEXT DEFAULT NULL,
                    successor_type TEXT DEFAULT NULL,
                    artifact_location TEXT DEFAULT NULL CHECK (artifact_location IS NULL OR json_valid(artifact_location)),
                    artifact_hash TEXT DEFAULT NULL,
                    run_id INTEGER,
                    details TEXT DEFAULT NULL CHECK (details IS NULL OR json_valid(details))
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
                log(f"Run {run_id} for node '{node_name}' already started. Ignoring duplicate start.", level="warning", logger=self.logger)
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
                    log(f"Run {run_id} for node '{node_name}' already ended. Ignoring duplicate end.", level="warning", logger=self.logger)
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
    
    def add_provenance_edge(
        self, 
        predecessor_name: str, predecessor_type: str, 
        successor_name: str, successor_type: str, 
        artifact_location: str,
        artifact_hash: str, 
        run_id: int = None
    ) -> None:
        with self.write_cursor() as cursor:
            query: sql = f"""
                INSERT OR IGNORE INTO provenance_graph (
                    predecessor_name, predecessor_type,
                    successor_name, successor_type, 
                    artifact_location,
                    artifact_hash, 
                    run_id
                ) 
                VALUES (?, ?, ?, ?, ?, ?, ?);
            """
            cursor.execute(query, 
                (
                    predecessor_name, predecessor_type, 
                    successor_name, successor_type, 
                    artifact_location,
                    artifact_hash, 
                    run_id
                )
            )
    