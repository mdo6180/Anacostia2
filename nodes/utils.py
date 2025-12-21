from enum import Enum


class Result(Enum):
    FAILURE = 0,
    SUCCESS = 1,
    ERROR = 2,
    
    def __repr__(self) -> str:
        status_words = {
            Result.FAILURE: "FAILURE",
            Result.SUCCESS: "SUCCESS",
            Result.ERROR: "ERROR",
        }
        return status_words[self]

    def __int__(self) -> int:
        return self.value
    
    def __eq__(self, other: 'Result') -> bool:
        if other.value == self.value:
            return True
        else:
            return False
    
    def __hash__(self) -> int:
        return super().__hash__()