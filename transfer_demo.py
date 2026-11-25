"""Demonstrate accepted transfer and source-free shift retrieval.

The recorder is closed before repair packets are accepted at the station.  The
published result is then reopened from the station database and retrieved by
its original result id.
"""

from __future__ import annotations

import json
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory

import station.repair_transfer as transfer
from station import open_station
from station.capture import receive
from station.closeout import close_shift
from station.device import DeviceRepository, issue_repair
from station.device.operations import RepairState
from station.review import retrieve
from transfer_scene import DEVICE, SHIFT, TRANSFER, advance, seed


def run(station_path: Path, recorder_path: Path) -> dict:
    with closing(open_station(station_path)) as db, \
            closing(DeviceRepository(recorder_path, DEVICE, 2)) as recorder:
        seed(db, recorder)

        boundary = transfer.prepare(db, SHIFT, TRANSFER)
        candidate = transfer.offer(recorder, boundary)
        accept_messages = transfer.accept(db, TRANSFER)

        # Candidate/acceptance are still in transit: the old named entry and
        # replay issue/reply stay pending without releasing R42.  Sequence 45
        # is outside the old shift and may reuse slot 1.
        assert issue_repair(recorder, 'R42').state is RepairState.PENDING
        assert advance(recorder).repair.state is RepairState.PENDING
        assert advance(recorder).repair.state is RepairState.PENDING
        assert advance(recorder).covered_sequence == 42

        for message in accept_messages:
            replies = transfer.deliver(recorder, message)
            for reply in replies:
                transfer.deliver(db, reply)
        transfer.deliver(db, candidate)
        assert transfer.status(db, TRANSFER) == 'accepted'
        assert transfer.status(recorder, TRANSFER) == 'accepted'

        r41 = transfer.repair(db, TRANSFER, 41, 'R41')
        r42 = transfer.repair(db, TRANSFER, 42, 'R42')
        r43 = transfer.repair(db, TRANSFER, 43, 'R43')
        assert (r41.state, r42.state, r43.state) == (
            RepairState.ISSUED, RepairState.ISSUED, RepairState.NOT_ISSUED,
        )

        # Stop/disconnect the source before any formal transferred intake.
        recorder.close()

        first_r41 = receive(db, r41.image, operation='R41')
        first_r42 = receive(db, r42.image, operation='R42')
        signed = close_shift(db, SHIFT)
        assert signed.status == 'published' and signed.publication is not None
        publication = signed.publication

    with closing(open_station(station_path)) as reopened:
        frozen = retrieve(reopened, 'shift/outgoing')
        reopened_status = transfer.status(reopened, TRANSFER)
        assert frozen == publication
    assert reopened_status == 'accepted'

    statement = frozen.statement
    origins = [
        {
            'sequence': row.sequence,
            'state': row.state,
            'slot': None if row.receipt is None else row.receipt.packet.slot,
            'micrometers_um': None if row.receipt is None else row.receipt.packet.micrometers,
            'operation': None if row.receipt is None else row.receipt.operation,
            'raw': None if row.receipt is None else row.receipt.packet.raw.hex(),
        }
        for row in statement.positions
    ]
    actual = {
        'result_id': statement.result_id,
        'order': [row.sequence for row in statement.positions],
        'origins': origins,
        'received_count': statement.stats.received_count,
        'minimum_um': statement.stats.minimum_um,
        'maximum_um': statement.stats.maximum_um,
        'missing': list(statement.stats.missing),
        'first_receipt': frozen.first_receipt.receipt_id,
        'station_status_after_reopen': reopened_status,
        'receipt_ids': {
            'R41': first_r41.receipt_id,
            'R42': first_r42.receipt_id,
        },
    }
    expected = {
        'result_id': 'shift/outgoing',
        'order': [41, 42, 43],
        'slots': [0, 1, None],
        'readings_um': [7, 11, None],
        'operations': ['R41', 'R42', None],
        'received_count': 2,
        'minimum_um': 7,
        'maximum_um': 11,
        'missing': [43],
        'first_receipt': 'first/shift/outgoing',
    }
    assert actual['result_id'] == expected['result_id']
    assert actual['order'] == expected['order']
    assert [item['slot'] for item in actual['origins']] == expected['slots']
    assert [item['micrometers_um'] for item in actual['origins']] == expected['readings_um']
    assert [item['operation'] for item in actual['origins']] == expected['operations']
    for key in ('received_count', 'minimum_um', 'maximum_um', 'missing', 'first_receipt'):
        assert actual[key] == expected[key], (key, actual[key], expected[key])
    return {'expected': expected, 'actual': actual}


def main() -> None:
    with TemporaryDirectory(prefix='repair-transfer-demo-') as directory:
        root = Path(directory)
        result = run(root / 'station.sqlite', root / 'recorder.sqlite')
    scenario = {
        'case_id': 'accepted-repair-transfer-source-stopped',
        'input': {
            'shift': SHIFT,
            'transfer_id': TRANSFER,
            'device': DEVICE,
            'range': [41, 43],
            'cutoff': {'R41': 'issued-before-offer', '42': 'readable-slot-1', '43': 'covered-never-issued'},
        },
        **result,
        'oracle': 'R41 按原号认回，R42 用出让截点槽 1 的原包断源补取；43 是唯一确证覆盖且从未交出的缺测。',
    }
    print('POINTWISE_SCENARIO ' + json.dumps(scenario, ensure_ascii=False, sort_keys=True))


if __name__ == '__main__':
    main()
