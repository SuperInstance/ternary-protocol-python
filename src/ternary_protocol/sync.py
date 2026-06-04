"""Diff-based synchronization protocol for ternary data."""

from __future__ import annotations
from dataclasses import dataclass


@dataclass
class DiffOp:
    """A single diff operation: set position `index` to `value`."""
    index: int
    value: int  # must be -1, 0, or +1


class SyncProtocol:
    """Synchronize ternary data between nodes using diffs.

    Instead of sending full payloads, nodes exchange diffs (changes only),
    reducing bandwidth for incremental updates.
    """

    def __init__(self) -> None:
        self._versions: dict[str, int] = {}
        self._states: dict[str, list[int]] = {}
        self._diff_log: dict[str, list[list[DiffOp]]] = {}

    def register(self, node_id: str, initial_state: list[int], version: int = 0) -> None:
        """Register a node with its initial state."""
        self._validate_state(initial_state)
        self._states[node_id] = list(initial_state)
        self._versions[node_id] = version
        self._diff_log[node_id] = []

    def get_state(self, node_id: str) -> list[int]:
        """Get current state of a node."""
        return list(self._states[node_id])

    def get_version(self, node_id: str) -> int:
        """Get current version of a node's state."""
        return self._versions[node_id]

    def compute_diff(self, from_id: str, to_id: str) -> list[DiffOp]:
        """Compute diff operations to transform from_id's state to to_id's state."""
        src = self._states[from_id]
        dst = self._states[to_id]
        if len(src) != len(dst):
            raise ValueError("States must have same length for diffing")
        return [DiffOp(i, dst[i]) for i in range(len(src)) if src[i] != dst[i]]

    def apply_diff(self, node_id: str, diff: list[DiffOp], expected_version: int | None = None) -> int:
        """Apply a diff to a node's state. Returns new version."""
        if expected_version is not None and self._versions[node_id] != expected_version:
            raise ValueError(
                f"Version conflict: expected {expected_version}, "
                f"got {self._versions[node_id]}"
            )
        for op in diff:
            if op.value not in (-1, 0, 1):
                raise ValueError(f"Invalid trit in diff: {op.value!r}")
            if op.index < 0 or op.index >= len(self._states[node_id]):
                raise IndexError(f"Diff index out of range: {op.index}")
            self._states[node_id][op.index] = op.value
        self._versions[node_id] += 1
        self._diff_log[node_id].append(diff)
        return self._versions[node_id]

    def sync(self, source_id: str, target_id: str) -> list[DiffOp]:
        """Synchronize target to match source state. Returns the applied diff."""
        diff = self.compute_diff(target_id, source_id)
        self.apply_diff(target_id, diff)
        return diff

    @property
    def diff_history(self) -> dict[str, list[list[DiffOp]]]:
        return dict(self._diff_log)

    @staticmethod
    def _validate_state(state: list[int]) -> None:
        for v in state:
            if v not in (-1, 0, 1):
                raise ValueError(f"Invalid ternary value: {v!r}")
