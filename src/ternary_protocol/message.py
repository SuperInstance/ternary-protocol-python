"""Ternary message structure for protocol communication."""

from __future__ import annotations
import time
import hashlib
from dataclasses import dataclass, field
from typing import Any


@dataclass
class TernaryMessage:
    """A message carrying ternary-encoded payloads.

    Messages have a header (source, destination, type) and a ternary payload.
    """

    source: str
    destination: str
    payload: list[int]  # ternary values (-1, 0, +1)
    msg_type: str = "data"
    timestamp: float = field(default_factory=time.time)
    msg_id: str = ""

    def __post_init__(self) -> None:
        if not self.msg_id:
            content = f"{self.source}:{self.destination}:{self.timestamp}"
            self.msg_id = hashlib.md5(content.encode()).hexdigest()[:12]
        self._validate_payload()

    def _validate_payload(self) -> None:
        for v in self.payload:
            if v not in (-1, 0, 1):
                raise ValueError(f"Invalid ternary value in payload: {v!r}")

    @property
    def size(self) -> int:
        """Number of ternary digits in payload."""
        return len(self.payload)

    def checksum(self) -> int:
        """Simple checksum: sum of payload values modulo 3."""
        return sum(v + 1 for v in self.payload) % 3

    def to_dict(self) -> dict[str, Any]:
        """Serialize message to dictionary."""
        return {
            "msg_id": self.msg_id,
            "source": self.source,
            "destination": self.destination,
            "msg_type": self.msg_type,
            "payload": self.payload,
            "timestamp": self.timestamp,
            "checksum": self.checksum(),
        }

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> TernaryMessage:
        """Deserialize from dictionary."""
        return cls(
            source=d["source"],
            destination=d["destination"],
            payload=d["payload"],
            msg_type=d.get("msg_type", "data"),
            timestamp=d.get("timestamp", time.time()),
            msg_id=d.get("msg_id", ""),
        )

    def is_broadcast(self) -> bool:
        """Check if this is a broadcast message."""
        return self.destination == "*"
