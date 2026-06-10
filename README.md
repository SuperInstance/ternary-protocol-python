# ternary-protocol-python

![Python](https://img.shields.io/badge/language-Python%203.10-blue)
![License](https://img.shields.io/badge/license-MIT-green)
![SuperInstance](https://img.shields.io/badge/fleet-SuperInstance-orange)

Message passing, synchronization, and handshake protocols for ternary distributed systems. All payloads are balanced trits {-1, 0, +1}.

## Why This Exists

Distributed systems built on ternary representations need their own communication layer. Standard protocols operate on bytes; this library operates on trits. Messages carry ternary-encoded payloads, nodes authenticate with ternary secrets, and state synchronization exchanges diffs — not full state — to minimize bandwidth in {-1, 0, +1} space.

The conservation law γ + η = C governs the protocol: the total information budget across the bus is fixed, so synchronization must be efficient. Diff-based sync (sending only changed trits) keeps η low.

## Installation

```bash
pip install ternary-protocol
```

Requires Python ≥ 3.10.

## Usage

### Create and send ternary messages

```python
from ternary_protocol import TernaryMessage, MessageBus

bus = MessageBus()

# Subscribe handlers
bus.subscribe("agent-A", lambda msg: print(f"A got: {msg.payload}"))
bus.subscribe("agent-B", lambda msg: print(f"B got: {msg.payload}"))

# Point-to-point
msg = TernaryMessage(
    source="agent-A",
    destination="agent-B",
    payload=[1, 0, -1, 1, 0, 0, -1, 1],
    msg_type="data"
)
recipients = bus.send(msg)  # 1

# Broadcast
broadcast = TernaryMessage(
    source="agent-A",
    destination="*",
    payload=[-1, -1, 1, 0],
    msg_type="signal"
)
recipients = bus.send(broadcast)  # 2

# Message properties
msg.checksum()     # sum of (v+1) mod 3
msg.size           # 8
msg.is_broadcast() # False
msg.to_dict()      # serialize
```

### Topic-based multicast

```python
bus.join_topic("agent-A", "evolution")
bus.join_topic("agent-B", "evolution")

topic_msg = TernaryMessage(
    source="controller",
    destination="evolution",
    payload=[1, 1, -1, 0, 1],
)
bus.send(topic_msg)  # delivered to A and B
```

### Encode/decode ternary payloads

```python
from ternary_protocol import Payload

# Integer → balanced ternary
p = Payload.from_int(42, width=8)
p.data   # [0, 1, 1, -1, 0, 0, 0, 0] (least significant first)
p.to_int()  # 42

# Bytes → ternary
p = Payload.from_bytes(b"Hi")
p.length  # 16 trits (8 per byte)
p.to_bytes()  # b"Hi"

# String encoding
p = Payload.from_string("OK")
p.to_string()  # "OK"
```

### Synchronize state between nodes

```python
from ternary_protocol import SyncProtocol

sync = SyncProtocol()

# Register nodes with initial ternary state
sync.register("node-1", [1, 0, -1, 1, 0])
sync.register("node-2", [1, 0, 0, -1, -1])

# Compute diff (what changed from node-1 to node-2)
diff = sync.compute_diff("node-1", "node-2")
# [DiffOp(index=2, value=0), DiffOp(index=3, value=-1), DiffOp(index=4, value=-1)]

# Apply diff with version check
sync.apply_diff("node-1", diff, expected_version=0)  # returns new version: 1

# Full sync: make target match source
sync.sync("node-2", "node-1")  # node-2 now matches node-1

# Version tracking
sync.get_version("node-1")  # 1
sync.diff_history  # per-node log of all applied diffs
```

### Authenticate nodes with ternary handshakes

```python
from ternary_protocol import Handshake

hs = Handshake(
    node_a="controller",
    node_b="worker-1",
    secret=[1, 0, -1, 1],
    timeout_seconds=30.0
)

# 3-phase handshake
request = hs.request()     # Phase 1: init
# {"type": "handshake_request", "from": "controller", "to": "worker-1", ...}

challenge = hs.challenge()  # Phase 2: challenge token
# {"type": "handshake_challenge", "token": [0, 1, -1, 1], ...}

confirm = hs.confirm(challenge["token"])  # Phase 3: verify
# {"type": "handshake_confirm", "from": "controller", ...}

hs.is_established  # True
hs.phase           # 3
```

## API Reference

### TernaryMessage

```python
@dataclass
class TernaryMessage:
    source: str
    destination: str
    payload: list[int]        # must be {-1, 0, +1}
    msg_type: str = "data"
    timestamp: float          # auto-set
    msg_id: str               # auto-generated MD5
    
    @property
    def size(self) -> int
    def checksum(self) -> int              # sum of (v+1) mod 3
    def is_broadcast(self) -> bool         # destination == "*"
    def to_dict(self) -> dict
    @classmethod
    def from_dict(cls, d: dict) -> TernaryMessage
```

### Payload

```python
class Payload:
    def __init__(self, data: list[int] | None = None)
    
    @property
    def data(self) -> list[int]
    @property
    def length(self) -> int
    
    @classmethod
    def from_int(cls, value: int, width: int = 8) -> Payload
    def to_int(self) -> int
    
    @classmethod
    def from_bytes(cls, data: bytes, trits_per_byte: int = 8) -> Payload
    def to_bytes(self, trits_per_byte: int = 8) -> bytes
    
    @classmethod
    def from_string(cls, text: str) -> Payload
    def to_string(self) -> str
```

Balanced ternary encoding: trit −1 represents digit 2 in mod-3, enabling compact integer representation (least significant trit first).

### MessageBus

```python
class MessageBus:
    def subscribe(self, subscriber_id: str, handler: Callable[[TernaryMessage], None]) -> None
    def unsubscribe(self, subscriber_id: str) -> None
    def join_topic(self, subscriber_id: str, topic: str) -> None
    def leave_topic(self, subscriber_id: str, topic: str) -> None
    def send(self, message: TernaryMessage) -> int  # returns recipient count
    
    @property
    def history(self) -> list[TernaryMessage]
    def history_for(self, subscriber_id: str) -> list[TernaryMessage]
```

Delivery modes: point-to-point (destination = subscriber ID), multicast (destination = topic name), broadcast (destination = `*`). History capped at 1000 messages.

### SyncProtocol

```python
@dataclass
class DiffOp:
    index: int
    value: int   # must be {-1, 0, +1}

class SyncProtocol:
    def register(self, node_id: str, initial_state: list[int], version: int = 0) -> None
    def get_state(self, node_id: str) -> list[int]
    def get_version(self, node_id: str) -> int
    def compute_diff(self, from_id: str, to_id: str) -> list[DiffOp]
    def apply_diff(self, node_id: str, diff: list[DiffOp], expected_version: int | None = None) -> int
    def sync(self, source_id: str, target_id: str) -> list[DiffOp]
    
    @property
    def diff_history(self) -> dict[str, list[list[DiffOp]]]
```

`apply_diff` with `expected_version` raises `ValueError` on version conflict (optimistic locking).

### Handshake

```python
@dataclass
class Handshake:
    node_a: str
    node_b: str
    secret: Sequence[int]           # ternary shared secret
    timeout_seconds: float = 30.0
    
    @property
    def phase(self) -> int          # 0=init, 1=requested, 2=challenged, 3=confirmed
    @property
    def is_established(self) -> bool
    
    def request(self) -> dict       # Phase 1
    def challenge(self) -> dict     # Phase 2 (generates token from secret)
    def confirm(self, token: list[int]) -> dict  # Phase 3 (verifies token)
```

Token generation is deterministic from the shared secret via BLAKE2b hashing, so both nodes derive the same challenge token independently.

## Architecture

```
ternary-protocol-python/
├── src/ternary_protocol/
│   ├── __init__.py       # Public API re-exports
│   ├── message.py        # TernaryMessage — header + {-1,0,+1} payload
│   ├── payload.py        # Payload — int/bytes/string ↔ ternary encoding
│   ├── bus.py            # MessageBus — point-to-point, multicast, broadcast
│   ├── sync.py           # SyncProtocol + DiffOp — diff-based state sync
│   ├── handshake.py      # Handshake — 3-phase auth with ternary secrets
│   └── tests/
│       └── test_protocol.py
├── pyproject.toml
└── docs/
```

```
┌─────────────┐     ┌──────────────┐     ┌─────────────┐
│  Payload    │────→│    Message    │────→│  MessageBus │
│  (encoding) │     │  (envelope)  │     │  (routing)  │
└─────────────┘     └──────────────┘     └──────┬──────┘
                                                 │
                    ┌──────────────┐              │
                    │  Handshake   │              │
                    │  (auth)      │              │
                    └──────┬───────┘              │
                           │                      ↓
                    ┌──────────────┐     ┌──────────────┐
                    │  SyncProto   │     │  Subscribers │
                    │  (diff sync) │     │  (handlers)  │
                    └──────────────┘     └──────────────┘
```

## Related SuperInstance Crates

- **ternary-fitness-python** — Fitness evaluation for the genomes carried in these messages
- **conservation-spectral-topology** — Verifies γ + η = C across the distributed nodes
- **metal-lathe** — Research wheel that generates hypotheses about ternary protocol behavior

## License

MIT
