"""Independent databases and replayable messages for the shift transfer example."""

import argparse
import json
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory

from demo import DEVICE, sample_event
from station import open_station
from station.capture import receipts_for_range, receive, request_repair
from station.closeout import close_shift
from station.device import DeviceRepository, ReplaySession, issue_repair, parse_recording, poll, query_repair
from station.device.slots import position_availability, read_position
from station.shift import fix_bounds, open_shift

SHIFT = "outgoing"
TRANSFER = "outgoing-transfer"
EVENTS = parse_recording(json.dumps([
    sample_event(41, 0, 7),
    sample_event(42, 1, 11),
    sample_event(43, 0, 9),
    sample_event(44, 0, 19),
    {"kind": "issue", "operation": "R42"},
    {"kind": "reply", "operation": "R42"},
    sample_event(45, 1, 23),
]))


class Courier:
    def __init__(self):
        self.batches = {}

    def hold(self, label, *messages):
        if label in self.batches:
            raise ValueError("courier label already used")
        if any(not isinstance(message, bytes) for message in messages):
            raise TypeError("courier accepts bytes")
        self.batches[label] = messages

    def deliver(self, label, endpoint, apply):
        outgoing = []
        for message in self.batches[label]:
            outgoing.extend(apply(endpoint, message))
        return tuple(outgoing)


def advance(recorder):
    return poll(ReplaySession(recorder, EVENTS))


def seed(database, recorder):
    open_shift(database, SHIFT, DEVICE, 41)
    fix_bounds(database, SHIFT, 43)
    advance(recorder)
    advance(recorder)
    request_repair(database, SHIFT, DEVICE, 41, "R41", source=recorder)
    request_repair(database, SHIFT, DEVICE, 42, "R42", source=recorder)
    request_repair(database, SHIFT, DEVICE, 43, "R43")
    issued = issue_repair(recorder, "R41")
    assert issued.state.value == "issued" and issued.image.sequence == 41
    assert advance(recorder).covered_sequence == 41
    assert advance(recorder).covered_sequence == 43
    return issued.image


def verify_v1(station_path, recorder_path, *, reopen=False):
    with closing(open_station(station_path)) as database, closing(DeviceRepository(recorder_path, DEVICE, 2)) as recorder:
        original = seed(database, recorder)
        availability = [position_availability(recorder, seq).value for seq in (41, 42, 43)]
        assert availability == ["covered", "readable", "covered"]
        assert query_repair(recorder, "R43").state.value == "unknown"
        assert close_shift(database, SHIFT, source=recorder).pending == (41, 42, 43)
        try:
            receive(database, original, operation="R41")
        except ValueError as error:
            assert str(error) == "repair reply requires its source device"
        else:
            raise AssertionError("ordinary R41 was accepted without its source")

        online = receive(database, original, operation="R41", source=recorder)
        assert online.operation == "R41" and online.packet.raw == original.raw
        copied = receive(database, read_position(recorder, 42))
        assert copied.operation is None
        assert close_shift(database, SHIFT).pending == (43,)

        issue = advance(recorder)
        assert issue.repair.state.value == "issued"
        assert advance(recorder).image == issue.repair.image
        assert advance(recorder).covered_sequence == 42
        request_repair(database, SHIFT, DEVICE, 42, "after-cover", source=recorder)
        after_cover = issue_repair(recorder, "after-cover").state.value
        assert after_cover == "not_issued"
        assert read_position(recorder, 42) is None
        assert advance(recorder) is None

    courier = Courier()
    courier.hold("sample", b"delayed control message")
    observed = []

    def apply(endpoint, message):
        endpoint.append(message)
        return ()

    assert courier.deliver("sample", observed, apply) == ()
    assert courier.deliver("sample", observed, apply) == ()
    assert observed == [b"delayed control message", b"delayed control message"]

    if reopen:
        with closing(open_station(station_path)) as database, closing(DeviceRepository(recorder_path, DEVICE, 2)) as recorder:
            assert query_repair(recorder, "R41").image == original
            assert query_repair(recorder, "R42") == issue.repair
            assert receipts_for_range(database, DEVICE, 41, 43) == (online, copied)
            assert close_shift(database, SHIFT).pending == (43,)
            assert advance(recorder) is None

    return {
        "at_offer_sequences": [41, 42, 43],
        "at_offer_availability": availability,
        "ordinary_offline_named_repair": "rejected",
        "ordinary_copied_receipt_operation": copied.operation,
        "ordinary_offline_pending": [43],
        "ordinary_replay_issue": issue.repair.state.value,
        "new_device_issue_after_cover": after_cover,
        "courier_deliveries": len(observed),
        "reopened": reopen,
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--memory", action="store_true")
    options = parser.parse_args()
    if options.memory:
        result = verify_v1(":memory:", ":memory:")
    else:
        with TemporaryDirectory(prefix="repair-transfer-") as directory:
            root = Path(directory)
            result = verify_v1(root / "station.sqlite", root / "recorder.sqlite", reopen=True)
    print(json.dumps(result, sort_keys=True))


if __name__ == "__main__":
    main()
