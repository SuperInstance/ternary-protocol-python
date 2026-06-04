"""ternary-protocol: Message passing and synchronization for ternary distributed systems."""

from .message import TernaryMessage
from .payload import Payload
from .bus import MessageBus
from .sync import SyncProtocol
from .handshake import Handshake

__all__ = [
    "TernaryMessage",
    "Payload",
    "MessageBus",
    "SyncProtocol",
    "Handshake",
]
