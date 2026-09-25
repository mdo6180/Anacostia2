from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Literal, Sequence

Hash = bytes
Side = Literal["left", "right"]


@dataclass(frozen=True)
class ProofEntry:
    side: Side
    hash: Hash


def sha256(data: bytes) -> Hash:
    return hashlib.sha256(data).digest()


# note: the "\x00" and "\x01" prefixes are used to domain-separate the leaf and node hashes, preventing second-preimage attacks.
def hash_leaf(data: bytes) -> Hash:
    """Domain-separated Merkle leaf hash."""
    return sha256(b"\x00" + data)


def hash_node(left: Hash, right: Hash) -> Hash:
    """Domain-separated Merkle internal-node hash."""
    return sha256(b"\x01" + left + right)


def build_merkle_levels(items: Sequence[bytes]) -> list[list[Hash]]:
    """
    Build all levels of a Merkle tree.

    Odd nodes are promoted unchanged to the next level.
    """
    if not items:
        raise ValueError("A Merkle tree requires at least one item")

    levels: list[list[Hash]] = [[hash_leaf(item) for item in items]]

    while len(levels[-1]) > 1:
        current = levels[-1]
        parent_level: list[Hash] = []

        for index in range(0, len(current), 2):
            left = current[index]

            if index + 1 < len(current):
                right = current[index + 1]
                parent_level.append(hash_node(left, right))
            else:
                parent_level.append(left)

        levels.append(parent_level)

    return levels


def merkle_root(items: Sequence[bytes]) -> Hash:
    return build_merkle_levels(items)[-1][0]


def generate_proof(
    items: Sequence[bytes],
    leaf_index: int,
) -> list[ProofEntry]:
    levels = build_merkle_levels(items)

    if not 0 <= leaf_index < len(levels[0]):
        raise IndexError("leaf_index is outside the tree")

    proof: list[ProofEntry] = []
    index = leaf_index

    for level in levels[:-1]:
        if index % 2 == 0:
            sibling_index = index + 1
            if sibling_index < len(level):
                proof.append(
                    ProofEntry(side="right", hash=level[sibling_index])
                )
        else:
            sibling_index = index - 1
            proof.append(
                ProofEntry(side="left", hash=level[sibling_index])
            )

        index //= 2

    return proof


def verify_proof(
    item: bytes,
    proof: Sequence[ProofEntry],
    expected_root: Hash,
) -> bool:
    current = hash_leaf(item)

    for entry in proof:
        if entry.side == "left":
            current = hash_node(entry.hash, current)
        elif entry.side == "right":
            current = hash_node(current, entry.hash)
        else:
            raise ValueError(f"Unknown proof side: {entry.side}")

    return current == expected_root