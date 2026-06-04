"""Tests for ternary-protocol package."""

import pytest
from ternary_protocol import TernaryMessage, Payload, MessageBus, SyncProtocol, Handshake


# --- TernaryMessage ---

class TestTernaryMessage:
    def test_create_message(self):
        m = TernaryMessage(source="A", destination="B", payload=[1, 0, -1])
        assert m.source == "A"
        assert m.destination == "B"
        assert m.payload == [1, 0, -1]

    def test_auto_generates_msg_id(self):
        m = TernaryMessage(source="A", destination="B", payload=[0])
        assert len(m.msg_id) == 12

    def test_invalid_payload_raises(self):
        with pytest.raises(ValueError, match="Invalid ternary"):
            TernaryMessage(source="A", destination="B", payload=[2])

    def test_size(self):
        m = TernaryMessage(source="A", destination="B", payload=[1, 0, -1, 1])
        assert m.size == 4

    def test_checksum(self):
        m = TernaryMessage(source="A", destination="B", payload=[1, 0, -1])
        # sum of (v+1): 2 + 1 + 0 = 3, 3 % 3 = 0
        assert m.checksum() == 0

    def test_serialization_roundtrip(self):
        m = TernaryMessage(source="A", destination="B", payload=[1, -1], msg_type="ctrl")
        d = m.to_dict()
        m2 = TernaryMessage.from_dict(d)
        assert m2.source == m.source
        assert m2.payload == m.payload
        assert m2.msg_type == m.msg_type

    def test_is_broadcast(self):
        m = TernaryMessage(source="A", destination="*", payload=[0])
        assert m.is_broadcast()

    def test_is_not_broadcast(self):
        m = TernaryMessage(source="A", destination="B", payload=[0])
        assert not m.is_broadcast()


# --- Payload ---

class TestPayload:
    def test_empty_payload(self):
        p = Payload()
        assert p.length == 0
        assert p.data == []

    def test_invalid_trit_raises(self):
        with pytest.raises(ValueError, match="Invalid trit"):
            Payload([2])

    def test_from_int_roundtrip(self):
        p = Payload.from_int(5, width=4)
        assert p.to_int() == 5

    def test_from_int_zero(self):
        p = Payload.from_int(0, width=4)
        assert p.data == [0, 0, 0, 0]
        assert p.to_int() == 0

    def test_from_int_negative_raises(self):
        with pytest.raises(ValueError):
            Payload.from_int(-1, width=4)

    def test_from_int_overflow(self):
        with pytest.raises(OverflowError):
            Payload.from_int(100, width=2)

    def test_bytes_roundtrip(self):
        data = b"hello"
        p = Payload.from_bytes(data)
        assert p.to_bytes() == data

    def test_xor(self):
        a = Payload([1, 0, -1])
        b = Payload([-1, 0, 1])
        result = a.xor(b)
        assert result.data == [-1, 0, -1]

    def test_xor_length_mismatch(self):
        a = Payload([1, 0])
        b = Payload([1])
        with pytest.raises(ValueError):
            a.xor(b)

    def test_hamming_distance(self):
        a = Payload([1, 0, -1])
        b = Payload([1, 1, -1])
        assert a.hamming_distance(b) == 1

    def test_hamming_same(self):
        a = Payload([1, 0, -1])
        assert a.hamming_distance(Payload([1, 0, -1])) == 0

    def test_equality(self):
        assert Payload([1, 0]) == Payload([1, 0])
        assert Payload([1, 0]) != Payload([0, 1])

    def test_repr(self):
        p = Payload([1, 0])
        assert "Payload" in repr(p)


# --- MessageBus ---

class TestMessageBus:
    def test_subscribe_and_send(self):
        bus = MessageBus()
        received = []
        bus.subscribe("B", lambda m: received.append(m))
        m = TernaryMessage(source="A", destination="B", payload=[1])
        bus.send(m)
        assert len(received) == 1
        assert received[0].payload == [1]

    def test_broadcast(self):
        bus = MessageBus()
        received_a = []
        received_b = []
        bus.subscribe("A", lambda m: received_a.append(m))
        bus.subscribe("B", lambda m: received_b.append(m))
        m = TernaryMessage(source="A", destination="*", payload=[0])
        assert bus.send(m) == 2
        assert len(received_a) == 1
        assert len(received_b) == 1

    def test_multicast(self):
        bus = MessageBus()
        received = []
        bus.subscribe("A", lambda m: received.append(m))
        bus.subscribe("B", lambda m: received.append(m))
        bus.subscribe("C", lambda m: received.append(m))
        bus.join_topic("A", "topic1")
        bus.join_topic("C", "topic1")
        m = TernaryMessage(source="X", destination="topic1", payload=[1])
        assert bus.send(m) == 2
        assert len(received) == 2

    def test_unsubscribe(self):
        bus = MessageBus()
        received = []
        bus.subscribe("A", lambda m: received.append(m))
        bus.unsubscribe("A")
        m = TernaryMessage(source="B", destination="A", payload=[0])
        assert bus.send(m) == 0

    def test_history(self):
        bus = MessageBus()
        bus.subscribe("A", lambda m: None)
        bus.send(TernaryMessage(source="B", destination="A", payload=[1]))
        assert len(bus.history) == 1

    def test_history_for(self):
        bus = MessageBus()
        bus.subscribe("A", lambda m: None)
        bus.subscribe("B", lambda m: None)
        bus.send(TernaryMessage(source="X", destination="A", payload=[1]))
        bus.send(TernaryMessage(source="X", destination="B", payload=[0]))
        assert len(bus.history_for("A")) == 1

    def test_subscriber_count(self):
        bus = MessageBus()
        bus.subscribe("A", lambda m: None)
        bus.subscribe("B", lambda m: None)
        assert bus.subscriber_count == 2

    def test_leave_topic(self):
        bus = MessageBus()
        bus.subscribe("A", lambda m: None)
        bus.join_topic("A", "t")
        assert "A" in bus.topic_members("t")
        bus.leave_topic("A", "t")
        assert "A" not in bus.topic_members("t")


# --- SyncProtocol ---

class TestSyncProtocol:
    def test_register_and_get(self):
        sp = SyncProtocol()
        sp.register("n1", [1, 0, -1])
        assert sp.get_state("n1") == [1, 0, -1]
        assert sp.get_version("n1") == 0

    def test_compute_diff(self):
        sp = SyncProtocol()
        sp.register("a", [1, 0, -1])
        sp.register("b", [1, 1, -1])
        diff = sp.compute_diff("a", "b")
        assert len(diff) == 1
        assert diff[0].index == 1
        assert diff[0].value == 1

    def test_apply_diff(self):
        from ternary_protocol.sync import DiffOp
        sp = SyncProtocol()
        sp.register("n", [1, 0, -1])
        v = sp.apply_diff("n", [DiffOp(1, 1)])
        assert v == 1
        assert sp.get_state("n") == [1, 1, -1]

    def test_version_conflict(self):
        from ternary_protocol.sync import DiffOp
        sp = SyncProtocol()
        sp.register("n", [1, 0, -1])
        with pytest.raises(ValueError, match="Version conflict"):
            sp.apply_diff("n", [DiffOp(0, -1)], expected_version=99)

    def test_sync_nodes(self):
        sp = SyncProtocol()
        sp.register("a", [1, 1, 1])
        sp.register("b", [-1, -1, -1])
        diff = sp.sync("a", "b")
        assert len(diff) == 3
        assert sp.get_state("b") == [1, 1, 1]

    def test_no_diff_when_identical(self):
        sp = SyncProtocol()
        sp.register("a", [1, 0, -1])
        sp.register("b", [1, 0, -1])
        assert sp.compute_diff("a", "b") == []

    def test_diff_history(self):
        from ternary_protocol.sync import DiffOp
        sp = SyncProtocol()
        sp.register("n", [1, 0, -1])
        sp.apply_diff("n", [DiffOp(0, -1)])
        assert len(sp.diff_history["n"]) == 1

    def test_invalid_state_raises(self):
        sp = SyncProtocol()
        with pytest.raises(ValueError):
            sp.register("n", [1, 2, 3])


# --- Handshake ---

class TestHandshake:
    def test_full_handshake(self):
        hs = Handshake(node_a="alice", node_b="bob")
        result = hs.full_handshake()
        assert result["verified"] is True
        assert hs.is_established

    def test_phases(self):
        hs = Handshake(node_a="alice", node_b="bob")
        assert hs.phase == 0
        hs.request()
        assert hs.phase == 1
        challenge = hs.challenge()
        assert hs.phase == 2
        hs.confirm(challenge["token"])
        assert hs.phase == 3

    def test_double_request_raises(self):
        hs = Handshake(node_a="a", node_b="b")
        hs.request()
        with pytest.raises(RuntimeError):
            hs.request()

    def test_confirm_without_challenge_raises(self):
        hs = Handshake(node_a="a", node_b="b")
        with pytest.raises(RuntimeError):
            hs.confirm([1, 0])

    def test_wrong_token_fails(self):
        hs = Handshake(node_a="a", node_b="b")
        hs.request()
        hs.challenge()
        with pytest.raises(ValueError, match="verification failed"):
            hs.confirm([9, 9, 9, 9, 9, 9, 9, 9])

    def test_invalid_secret_raises(self):
        with pytest.raises(ValueError):
            Handshake(node_a="a", node_b="b", secret=[1, 2, 3])

    def test_no_timeout_initially(self):
        hs = Handshake(node_a="a", node_b="b")
        assert not hs.check_timeout()
