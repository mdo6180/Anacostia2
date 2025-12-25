import os
import io
from dataclasses import dataclass
from typing import Optional, Any, Iterable


@dataclass(frozen=True)
class ArtifactPath(os.PathLike):
    base_path: str
    path: str
    event: str

    def __fspath__(self) -> str:
        # Critical: makes this object path-like
        # Optional: mark as USED on any implicit filesystem usage.
        print(f"{self.path} marked as {self.event}")
        return os.path.join(self.base_path, self.path)


"""
if __name__ == "__main__":
    ap = ArtifactPath("base_dir", "artifact.txt", "CREATED")
    with open(ap, 'w') as f:
        f.write("Test content.")
"""