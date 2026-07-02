from pathlib import Path
import time

from anacostia.utils.connection import ConnectionManager

sql = str   # alias of the str type for syntax highlighting using the Python Inline Source Syntax Highlighting extension by Sam Willis in VSCode.



tests_path = Path("./testing_artifacts")
tests_path.mkdir(exist_ok=True, parents=True)
db_path = tests_path / "latlong.db"


def setup_database(db_path: Path):
    conn = ConnectionManager(db_path=str(db_path))
    print(f"Database created at: {db_path}")

    with conn.write_cursor() as cursor:
        query: sql = f"""
            CREATE TABLE IF NOT EXISTS latlong (
                latitude REAL,
                longitude REAL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );
        """
        cursor.execute(query)

    print("Table 'latlong' created successfully.")
    conn.close()


def run_test():
    conn = ConnectionManager(db_path=str(db_path))
    for i in range(10):
        with conn.write_cursor() as cursor:
            query: sql = f"""
                INSERT INTO latlong (latitude, longitude) VALUES (?, ?);
            """
            cursor.execute(query, (i * 10.0, i * 20.0))

        time.sleep(1)  # Sleep for a bit to create a time gap between entries

    conn.close()


if __name__ == "__main__":
    run_test()