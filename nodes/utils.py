import os
from dataclasses import dataclass


os.remove("artifact.txt") if os.path.exists("artifact.txt") else None


@dataclass(frozen=True)
class ArtifactPath(os.PathLike):
    path: str
    event: str

    def __fspath__(self) -> str:
        # Critical: makes this object path-like
        # Optional: mark as USED on any implicit filesystem usage.

        if self.event == "READ":
            print("check if artifact is already in the usage database")
            print("if not, log artifact into usage database as READ")
        elif self.event == "WRITE":
            print("log artifact into usage database as WRITE")

        return self.path
    


if __name__ == "__main__":
    ap = ArtifactPath("artifact.txt")
    with open(ap, 'w') as f:
        f.write("Test content.")
    
    with open(ap, 'r') as f:
        content = f.read()
        print(content)