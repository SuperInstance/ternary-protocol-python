"""Ternary-packed payload encoding and decoding."""

from __future__ import annotations


class Payload:
    """Encodes and decodes data as ternary sequences.

    Each value in the sequence is a balanced trit: -1, 0, or +1.
    Supports encoding integers, strings, and raw ternary data.
    """

    def __init__(self, data: list[int] | None = None) -> None:
        if data is not None:
            self._validate(data)
        self._data: list[int] = data or []

    @staticmethod
    def _validate(data: list[int]) -> None:
        for v in data:
            if v not in (-1, 0, 1):
                raise ValueError(f"Invalid trit: {v!r}")

    @property
    def data(self) -> list[int]:
        return list(self._data)

    @property
    def length(self) -> int:
        return len(self._data)

    @classmethod
    def from_int(cls, value: int, width: int = 8) -> Payload:
        """Encode a non-negative integer as balanced ternary (least significant first)."""
        if value < 0:
            raise ValueError("Only non-negative integers supported")
        if width < 1:
            raise ValueError("Width must be positive")
        trits = []
        remaining = value
        for _ in range(width):
            r = remaining % 3
            if r == 0:
                trits.append(0)
                remaining = remaining // 3
            elif r == 1:
                trits.append(1)
                remaining = (remaining - 1) // 3
            else:  # r == 2
                trits.append(-1)
                remaining = (remaining + 1) // 3
        if remaining > 0:
            raise OverflowError(f"Value {value} exceeds {width} trits")
        return cls(trits)

    def to_int(self) -> int:
        """Decode balanced ternary payload to integer."""
        result = 0
        power = 1
        for trit in self._data:
            result += trit * power
            power *= 3
        return result

    @classmethod
    def from_bytes(cls, data: bytes, trits_per_byte: int = 8) -> Payload:
        """Encode bytes as ternary (each byte → trits_per_byte trits)."""
        all_trits: list[int] = []
        for byte in data:
            p = cls.from_int(byte, width=trits_per_byte)
            all_trits.extend(p.data)
        return cls(all_trits)

    def to_bytes(self, trits_per_byte: int = 8) -> bytes:
        """Decode ternary payload back to bytes."""
        if self.length % trits_per_byte != 0:
            raise ValueError(f"Payload length {self.length} not divisible by {trits_per_byte}")
        result = []
        for i in range(0, self.length, trits_per_byte):
            chunk = self._data[i:i + trits_per_byte]
            val = 0
            power = 1
            for t in chunk:
                val += t * power
                power *= 3
            result.append(val)
        return bytes(result)

    def xor(self, other: Payload) -> Payload:
        """Ternary XOR (component-wise multiplication)."""
        if self.length != other.length:
            raise ValueError("Payloads must have same length for XOR")
        return Payload([a * b for a, b in zip(self._data, other._data)])

    def hamming_distance(self, other: Payload) -> int:
        """Count positions where payloads differ."""
        if self.length != other.length:
            raise ValueError("Payloads must have same length")
        return sum(a != b for a, b in zip(self._data, other._data))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Payload):
            return NotImplemented
        return self._data == other._data

    def __repr__(self) -> str:
        return f"Payload({self._data})"
