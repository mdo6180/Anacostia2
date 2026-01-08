from typing import List
import hashlib

from utils.connection import ConnectionManager



class BaseWatcherNode:
    def __init__(self, name: str, path: str):
        self.name = name
        self.conn_manager = ConnectionManager(db_path="./testing_artifacts/.anacostia/anacostia.db")
        self.run_id = 15
        self.global_usage_table_name = "artifact_usage_events"
        self.node_id = "a48a3c4568f3751bb4540f96fa707dc30200dec0833aec491590cc978f08a221"
        self.artifact_table_name = f"{name}_{self.node_id}_artifacts"
        self.hash_chunk_size = 1_048_576  # 1 MB
    
    def get_artifacts(self, filter: callable = None, state: str = None, limit: int = None) -> List[str]:
        with self.conn_manager.read_cursor() as cursor:
            if state is not None:
                cursor.execute(
                    f"""
                    SELECT artifact_path, artifact_hash FROM {self.artifact_table_name}
                    WHERE artifact_hash NOT IN (
                    SELECT DISTINCT artifact_hash FROM {self.global_usage_table_name} WHERE node_id = ? AND state = ?
                    );
                    """,
                    (self.node_id, state)
                )
            else:
                cursor.execute(
                    f"""
                    SELECT DISTINCT artifact_path, artifact_hash FROM {self.global_usage_table_name} WHERE node_id = ? AND state = 'used';
                    """,
                    (self.node_id,)
                )
            
            artifacts = cursor.fetchall()
            artifacts = [art[0] for art in artifacts]
        
        if filter is not None:
            accepted_artifacts = []
            for art in artifacts:
                if filter(art) is True:
                    accepted_artifacts.append(art)
        
        if limit is not None:
            accepted_artifacts = accepted_artifacts[:limit]
        
        return [art for art in accepted_artifacts]

    def mark_artifact_using(self, filepath: str, artifact_hash: str) -> None:
        with self.conn_manager.write_cursor() as cursor:
            cursor.execute(
                f"""
                INSERT INTO {self.global_usage_table_name} (artifact_path, artifact_hash, node_id, node_name, run_id, state, source)
                VALUES (?, ?, ?, ?, ?, ?, ?);
                """,
                (filepath, artifact_hash, self.node_id, self.name, self.run_id, "using", "detected")
            )
    
    def mark_artifact_used(self, filepath: str, artifact_hash: str) -> None:
        try:
            with self.conn_manager.write_cursor() as cursor:
                cursor.execute(
                    f"""
                    INSERT INTO {self.global_usage_table_name} (artifact_path, artifact_hash, node_id, node_name, run_id, state, source)
                    VALUES (?, ?, ?, ?, ?, ?, ?);
                    """,
                    (filepath, artifact_hash, self.node_id, self.name, self.run_id, "used", "detected")
                )
        except Exception as e:
            print(f"filepath {filepath}, node id {self.node_id}, node name {self.name}, run id {self.run_id} error: {e}")

    def hash_file(self, filepath: str) -> str:
        sha256 = hashlib.sha256()
        with open(filepath, 'rb') as f:
            while chunk := f.read(self.hash_chunk_size):
                sha256.update(chunk)
        return sha256.hexdigest()

    def load(self, path: str, mode: str = 'r'):
        try:
            hash = self.hash_file(path)
            self.mark_artifact_using(path, hash)
            with open(path, mode) as f:
                return f.read()
        except Exception as e:
            raise e
        finally:
            self.mark_artifact_used(path, hash)

if __name__ == "__main__":
    def filter_func(artifact):
        with open(artifact, 'r') as f:
            content = f.read()
            return int(content[-1]) % 2 == 0  # Keep only artifacts with last character as even number

    node = BaseWatcherNode(name="WatcherNode1", path="./testing_artifacts/data_store1")
    
    artifacts = node.get_artifacts(filter=filter_func, limit=5)
    for artifact in artifacts:
        content = node.load(artifact)
        print(f"Artifact: {artifact}, Content: {content}")



"""
# Requirements:
# - On any filesystem interaction (open, read, write), the artifact should be logged in the database.
# - Using the context manager `use_artifact`, the artifact should be marked as USING during the operation and then marked as USED afterward.
# - The ArtifactPath class should be path-like and compatible with standard file operations.
#   ap = ArtifactPath("artifact.txt")
    with ap.open('r') as f:
        # do some initial reading to see if artifact should be used
        with ap.use_artifact():
            new_ap = ArtifactPath("artifact2.txt", event="WRITE")
            with new_ap.open('w') as f2:
                f2.write("Test content.")
                # artifact2.txt should be logged as WRITE and linked to artifact.txt usage
# - Ensure that the logging to the database is thread-safe if multiple artifacts are being used concurrently 
"""