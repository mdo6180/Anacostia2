import sqlite3
from contextlib import contextmanager


class ConnectionManager:
    def __init__(self, db_path: str) -> None:
        self.connection = sqlite3.connect(db_path, check_same_thread=False, detect_types=sqlite3.PARSE_DECLTYPES)
        self.connection.execute("PRAGMA journal_mode=WAL;")
    
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

        with self.write_cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO run_events (node_name, node_id, run_id, timestamp, event_type)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, 'start');
                """,
                (node_name, node_id, run_id)
            )
            return cursor.lastrowid
    
    def end_run(self, node_name: str, run_id: int) -> int:
        node_id = self.get_node_id(node_name)

        with self.write_cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO run_events (node_name, node_id, run_id, timestamp, event_type)
                VALUES (?, ?, ?, CURRENT_TIMESTAMP, 'end');
                """,
                (node_name, node_id, run_id)
            )
            return cursor.lastrowid