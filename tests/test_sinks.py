from __future__ import annotations

import json
import os
import threading
import time
from pathlib import Path

import pytest

from spoolpi.core.reading import Reading
from spoolpi.core.retention import GapRecord
from spoolpi.sinks.base import AckSet, Envelope, Sink, SinkError, to_wire
from spoolpi.sinks.jsonl import JsonlSink
from spoolpi.sinks.memory import Hang, MemorySink, Partial, Raise, Reject


def batch(*seqs: int) -> list[Envelope]:
    return [Envelope("buf", s, Reading("s", float(s), None, s, s, "boot")) for s in seqs]


def test_memory_sink_satisfies_the_protocol() -> None:
    sink: Sink = MemorySink()
    sink.close()


def test_ackset_helpers() -> None:
    b = batch(1, 2, 3)
    assert AckSet.all(b).accepted == {1, 2, 3}
    assert len(AckSet.none()) == 0
    assert 2 in AckSet.of([2]) and 3 not in AckSet.of([2])


def test_accept_records_everything_including_resends() -> None:
    sink = MemorySink()
    assert sink.send(batch(1, 2)) == AckSet.of([1, 2])
    assert sink.send(batch(2)) == AckSet.of([2])
    assert sink.keys() == [("buf", 1), ("buf", 2), ("buf", 2)]


def test_reject_answers_but_takes_nothing() -> None:
    sink = MemorySink([Reject()])
    assert sink.send(batch(1, 2)) == AckSet.none()
    assert sink.received == []


def test_partial_first_n() -> None:
    sink = MemorySink([Partial.first(2)])
    assert sink.send(batch(5, 6, 7)) == AckSet.of([5, 6])
    assert sink.keys() == [("buf", 5), ("buf", 6)]


def test_partial_can_ack_a_non_prefix_subset() -> None:
    sink = MemorySink([Partial.where(lambda e: e.seq % 2 == 0)])
    assert sink.send(batch(1, 2, 3, 4)) == AckSet.of([2, 4])


def test_raise_records_nothing() -> None:
    sink = MemorySink([Raise(ConnectionError("uplink down"))])
    with pytest.raises(ConnectionError):
        sink.send(batch(1))
    assert sink.received == []


def test_script_then_default() -> None:
    sink = MemorySink([Raise(), Reject()], default=Partial.first(1))
    with pytest.raises(SinkError):
        sink.send(batch(1))
    assert sink.send(batch(1)) == AckSet.none()
    assert sink.send(batch(1, 2)) == AckSet.of([1])
    assert sink.send(batch(3, 4)) == AckSet.of([3])
    assert sink.calls == 4


@pytest.mark.parametrize("release", ["release_hung", "close"])
def test_hang_blocks_until_released_then_fails(release: str) -> None:
    sink = MemorySink([Hang()])
    errors: list[SinkError] = []

    def send() -> None:
        try:
            sink.send(batch(1))
        except SinkError as e:
            errors.append(e)

    t = threading.Thread(target=send)
    t.start()
    t.join(timeout=0.2)
    assert t.is_alive(), "send should still be hung"
    getattr(sink, release)()
    t.join(timeout=2)
    assert not t.is_alive() and len(errors) == 1
    assert sink.received == []


def test_jsonl_close_waits_for_a_running_send(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    sink = JsonlSink(tmp_path / "out.jsonl")
    real_fsync = os.fsync
    in_send = threading.Event()

    def slow_fsync(fd: int) -> None:
        in_send.set()
        time.sleep(0.3)
        real_fsync(fd)

    monkeypatch.setattr(os, "fsync", slow_fsync)
    results: list[AckSet] = []
    t = threading.Thread(target=lambda: results.append(sink.send(batch(1, 2))))
    t.start()
    in_send.wait(2)
    sink.close()  # must not close the fd while the send is still fsyncing it
    t.join(2)
    assert results == [AckSet.of([1, 2])]
    with pytest.raises(SinkError, match="closed"):
        sink.send(batch(3))
    lines = (tmp_path / "out.jsonl").read_text().splitlines()
    assert [json.loads(line)["seq"] for line in lines] == [1, 2]


def test_wire_form_of_a_gap() -> None:
    gap = GapRecord("s", "boot", 1, 9, "backpressure", 4)
    assert to_wire(Envelope("buf", 7, gap)) == {
        "buffer_id": "buf",
        "seq": 7,
        "type": "gap",
        "sensor_id": "s",
        "boot_id": "boot",
        "from_mono_ns": 1,
        "to_mono_ns": 9,
        "reason": "backpressure",
        "count": 4,
    }


def test_envelope_key() -> None:
    assert batch(9)[0].key == ("buf", 9)
