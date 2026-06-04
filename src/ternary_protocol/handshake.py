"""Handshake protocol for ternary distributed node negotiation."""

from __future__ import annotations
import hashlib
import time
from dataclasses import dataclass, field
from typing import Sequence


@dataclass
class Handshake:
    """Negotiates a connection between two ternary nodes.

    Performs a 3-phase handshake (request → challenge → confirm) with
    ternary-encoded tokens for authentication.
    """

    node_a: str
    node_b: str
    secret: Sequence[int] = field(default_factory=lambda: [1, 0, -1, 1])
    timeout_seconds: float = 30.0

    def __post_init__(self) -> None:
        self._validate_secret()
        self._phase: int = 0  # 0=init, 1=requested, 2=challenged, 3=confirmed
        self._challenge_token: list[int] | None = None
        self._started_at: float = 0.0

    def _validate_secret(self) -> None:
        for v in self.secret:
            if v not in (-1, 0, 1):
                raise ValueError(f"Invalid ternary value in secret: {v!r}")

    @property
    def phase(self) -> int:
        return self._phase

    @property
    def is_established(self) -> bool:
        return self._phase == 3

    def request(self) -> dict:
        """Phase 1: Initiate handshake."""
        if self._phase != 0:
            raise RuntimeError("Handshake already in progress")
        self._started_at = time.time()
        self._phase = 1
        return {
            "type": "handshake_request",
            "from": self.node_a,
            "to": self.node_b,
            "timestamp": self._started_at,
        }

    def challenge(self) -> dict:
        """Phase 2: Generate challenge based on shared secret."""
        if self._phase != 1:
            raise RuntimeError(f"Expected phase 1, got {self._phase}")
        # Generate challenge token derived from secret
        token = self._generate_token()
        self._challenge_token = token
        self._phase = 2
        return {
            "type": "handshake_challenge",
            "from": self.node_b,
            "to": self.node_a,
            "token": token,
        }

    def confirm(self, token: list[int]) -> dict:
        """Phase 3: Confirm by verifying challenge token."""
        if self._phase != 2:
            raise RuntimeError(f"Expected phase 2, got {self._phase}")
        expected = self._generate_token()
        if token != expected:
            raise ValueError("Challenge verification failed")
        self._phase = 3
        return {
            "type": "handshake_confirm",
            "from": self.node_a,
            "to": self.node_b,
            "verified": True,
        }

    def _generate_token(self) -> list[int]:
        """Generate a deterministic challenge token from the shared secret."""
        raw = "".join(str(v + 1) for v in self.secret)
        digest = hashlib.sha256(raw.encode()).hexdigest()
        # Map hex digits to ternary
        trits = []
        for ch in digest[:len(self.secret) * 2]:
            d = int(ch, 16)
            trits.append((d % 3) - 1)  # 0→-1, 1→0, 2→1
        return trits

    def check_timeout(self) -> bool:
        """Check if handshake has timed out."""
        if self._started_at == 0:
            return False
        return (time.time() - self._started_at) > self.timeout_seconds

    def full_handshake(self) -> dict:
        """Execute all three phases automatically (for testing/simulation)."""
        self.request()
        challenge = self.challenge()
        result = self.confirm(challenge["token"])
        return result
