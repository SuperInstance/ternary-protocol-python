"""Message bus supporting broadcast and multicast for ternary messages."""

from __future__ import annotations
from collections import defaultdict
from typing import Callable
from .message import TernaryMessage


class MessageBus:
    """A simple in-process message bus for ternary messages.

    Supports point-to-point, broadcast, and multicast (topic-based) delivery.
    """

    def __init__(self) -> None:
        self._subscribers: dict[str, list[Callable[[TernaryMessage], None]]] = defaultdict(list)
        self._topics: dict[str, set[str]] = defaultdict(set)  # topic -> set of subscriber ids
        self._history: list[TernaryMessage] = []
        self._max_history: int = 1000

    def subscribe(self, subscriber_id: str, handler: Callable[[TernaryMessage], None]) -> None:
        """Subscribe a handler for messages addressed to subscriber_id."""
        self._subscribers[subscriber_id].append(handler)

    def unsubscribe(self, subscriber_id: str) -> None:
        """Remove all handlers for a subscriber."""
        self._subscribers.pop(subscriber_id, None)
        # Remove from all topics
        for members in self._topics.values():
            members.discard(subscriber_id)

    def join_topic(self, subscriber_id: str, topic: str) -> None:
        """Add subscriber to a multicast topic."""
        self._topics[topic].add(subscriber_id)

    def leave_topic(self, subscriber_id: str, topic: str) -> None:
        """Remove subscriber from a multicast topic."""
        if topic in self._topics:
            self._topics[topic].discard(subscriber_id)

    def send(self, message: TernaryMessage) -> int:
        """Send a message. Returns number of recipients."""
        self._history.append(message)
        if len(self._history) > self._max_history:
            self._history = self._history[-self._max_history:]

        if message.is_broadcast():
            return self._broadcast(message)

        # Check if destination is a topic
        if message.destination in self._topics:
            return self._multicast(message.destination, message)

        # Point-to-point
        return self._deliver(message.destination, message)

    def _broadcast(self, message: TernaryMessage) -> int:
        count = 0
        for sub_id in self._subscribers:
            count += self._deliver(sub_id, message)
        return count

    def _multicast(self, topic: str, message: TernaryMessage) -> int:
        count = 0
        for sub_id in self._topics.get(topic, set()):
            count += self._deliver(sub_id, message)
        return count

    def _deliver(self, subscriber_id: str, message: TernaryMessage) -> int:
        handlers = self._subscribers.get(subscriber_id, [])
        for handler in handlers:
            handler(message)
        return len(handlers)

    @property
    def history(self) -> list[TernaryMessage]:
        return list(self._history)

    def history_for(self, subscriber_id: str) -> list[TernaryMessage]:
        """Get messages addressed to a specific subscriber."""
        return [
            m for m in self._history
            if m.destination == subscriber_id or m.is_broadcast()
        ]

    @property
    def subscriber_count(self) -> int:
        return len(self._subscribers)

    def topic_members(self, topic: str) -> set[str]:
        return set(self._topics.get(topic, set()))
