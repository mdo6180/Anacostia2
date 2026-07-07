from typing import TypeAlias
from dataclasses import dataclass


JsonValue: TypeAlias = (
    str
    | int
    | float
    | bool
    | None
    | list["JsonValue"]
    | dict[str, "JsonValue"]
)

JsonDict: TypeAlias = dict[str, JsonValue]


@dataclass(frozen=True)
class Artifact:
    location: JsonDict
    hash: str