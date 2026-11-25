"""Portable, revocable handover for one fixed but unpublished shift.

The module intentionally keeps its wire messages opaque to callers.  They are
JSON envelopes encoded as UTF-8 bytes so that a courier can move them between
the station and recorder databases.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from typing import Any

from station import require_integer, require_name
from station.capture.receipts import receipts_for_range
from station.capture.repository import CaptureRepository, StoredRepairRequest
from station.device.operations import RepairOutcome, RepairState
from station.device.repository import DeviceRepository
from station.device.slots import PositionAvailability, SlotImage, position_availability, read_position
from station.shift.roster import lookup_shift


PROTOCOL_VERSION = 1
STATUSES = ('pending', 'accepted', 'withdrawn')


# ---------------------------------------------------------------------------
# Schema and endpoint helpers
# ---------------------------------------------------------------------------

def initialize_station_schema(database: sqlite3.Connection) -> None:
    database.executescript('''
        CREATE TABLE IF NOT EXISTS repair_transfers (
            transfer_id TEXT PRIMARY KEY,
            shift_id TEXT NOT NULL REFERENCES shifts (shift_id),
            device TEXT NOT NULL,
            start INTEGER NOT NULL CHECK (start >= 0),
            "end" INTEGER NOT NULL CHECK ("end" >= start),
            status TEXT NOT NULL CHECK (status IN ('pending', 'accepted', 'withdrawn')),
            accept_intent INTEGER NOT NULL DEFAULT 0 CHECK (accept_intent IN (0, 1)),
            snapshot TEXT,
            candidate BLOB
        );
        CREATE INDEX IF NOT EXISTS repair_transfers_by_shift
            ON repair_transfers (shift_id);
        CREATE INDEX IF NOT EXISTS repair_transfers_by_range
            ON repair_transfers (device, start, "end");
    ''')


def initialize_recorder_schema(database: sqlite3.Connection) -> None:
    database.executescript('''
        CREATE TABLE IF NOT EXISTS repair_transfers (
            transfer_id TEXT PRIMARY KEY,
            shift_id TEXT NOT NULL,
            device TEXT NOT NULL,
            start INTEGER NOT NULL CHECK (start >= 0),
            "end" INTEGER NOT NULL CHECK ("end" >= start),
            status TEXT NOT NULL CHECK (status IN ('pending', 'accepted', 'withdrawn')),
            withdraw_intent INTEGER NOT NULL DEFAULT 0 CHECK (withdraw_intent IN (0, 1)),
            snapshot TEXT NOT NULL,
            candidate BLOB NOT NULL
        );
        CREATE INDEX IF NOT EXISTS device_repair_transfers_by_range
            ON repair_transfers (device, start, "end");
    ''')


def _endpoint_kind(endpoint: Any) -> str:
    if isinstance(endpoint, DeviceRepository):
        return 'recorder'
    if isinstance(endpoint, sqlite3.Connection):
        return 'station'
    raise ValueError('endpoint must be a station connection or DeviceRepository')


def _connection(endpoint: Any) -> sqlite3.Connection:
    return endpoint.connection if isinstance(endpoint, DeviceRepository) else endpoint


def _initialize(endpoint: Any, kind: str) -> None:
    connection = _connection(endpoint)
    if kind == 'station':
        initialize_station_schema(connection)
    else:
        initialize_recorder_schema(connection)


@contextmanager
def _transaction(endpoint: Any, kind: str):
    if kind == 'station':
        context = CaptureRepository(_connection(endpoint)).transaction()
    else:
        context = endpoint.transaction()
    with context:
        yield _connection(endpoint)


def _validate_identity(transfer_id: object) -> str:
    return require_name(transfer_id, 'transfer id')


# ---------------------------------------------------------------------------
# Opaque portable messages
# ---------------------------------------------------------------------------

_BINDING_FIELDS = ('transfer_id', 'shift_id', 'device', 'start', 'end')
_MESSAGE_FIELDS = {'v', 'audience', 'kind', *(_BINDING_FIELDS), 'data'}


def _encode_message(
    audience: str,
    kind: str,
    binding: sqlite3.Row | dict[str, Any],
    data: dict[str, Any] | None = None,
) -> bytes:
    document = {
        'v': PROTOCOL_VERSION,
        'audience': audience,
        'kind': kind,
        'transfer_id': binding['transfer_id'],
        'shift_id': binding['shift_id'],
        'device': binding['device'],
        'start': binding['start'],
        'end': binding['end'],
        'data': data or {},
    }
    return json.dumps(
        document, ensure_ascii=False, separators=(',', ':'), sort_keys=True,
    ).encode('utf-8')


def _parse_message(
    message: object,
    endpoint: Any,
    *,
    audience: str | None = None,
    kind: str | None = None,
) -> dict[str, Any]:
    if not isinstance(message, bytes):
        raise ValueError('transfer message must be bytes')
    try:
        document = json.loads(message.decode('utf-8'))
    except (UnicodeError, ValueError, RecursionError) as error:
        raise ValueError('malformed transfer message') from error

    if not isinstance(document, dict) or set(document) != _MESSAGE_FIELDS:
        raise ValueError('malformed transfer message fields')
    if document.get('v') != PROTOCOL_VERSION:
        raise ValueError('unsupported transfer message version')
    if audience is not None and document.get('audience') != audience:
        raise ValueError('transfer message is bound to the other endpoint role')
    if kind is not None and document.get('kind') != kind:
        raise ValueError('unexpected transfer message kind')
    if not isinstance(document.get('data'), dict):
        raise ValueError('malformed transfer message data')

    try:
        require_name(document['transfer_id'], 'transfer id')
        require_name(document['shift_id'], 'shift id')
        require_name(document['device'], 'device')
        require_integer(document['start'], 'range start')
        require_integer(document['end'], 'range end')
    except ValueError as error:
        raise ValueError(f'invalid transfer binding: {error}') from error
    if document['end'] < document['start']:
        raise ValueError('transfer range end precedes its start')

    endpoint_kind = _endpoint_kind(endpoint)
    expected_audience = 'recorder' if endpoint_kind == 'recorder' else 'station'
    if document['audience'] != expected_audience:
        raise ValueError('transfer message does not address this endpoint role')
    if endpoint_kind == 'recorder' and document['device'] != endpoint.device:
        raise ValueError('transfer message is bound to a different recorder')
    return document


def _binding_matches(row: sqlite3.Row, document: dict[str, Any]) -> bool:
    return all(row[field] == document[field] for field in _BINDING_FIELDS)


def _check_binding(row: sqlite3.Row | None, document: dict[str, Any]) -> None:
    if row is None:
        raise ValueError('transfer identity is not registered at this endpoint')
    if not _binding_matches(row, document):
        raise ValueError('transfer message conflicts with its registered binding')


def _station_row(database: sqlite3.Connection, transfer_id: str) -> sqlite3.Row | None:
    return database.execute(
        'SELECT * FROM repair_transfers WHERE transfer_id = ?', (transfer_id,),
    ).fetchone()


def _recorder_row(recorder: DeviceRepository, transfer_id: str) -> sqlite3.Row | None:
    return recorder.connection.execute(
        'SELECT * FROM repair_transfers WHERE transfer_id = ?', (transfer_id,),
    ).fetchone()


def _accept_message(row: sqlite3.Row) -> bytes:
    return _encode_message('recorder', 'accept', row)


def _withdraw_message(row: sqlite3.Row) -> bytes:
    return _encode_message('station', 'withdraw', row)


def _withdraw_ack_message(row: sqlite3.Row) -> bytes:
    return _encode_message('recorder', 'withdraw_ack', row, {'status': 'withdrawn'})


# ---------------------------------------------------------------------------
# Offer snapshots
# ---------------------------------------------------------------------------

def _build_snapshot(recorder: DeviceRepository, start: int, end: int) -> dict[str, Any]:
    positions = []
    for sequence in range(start, end + 1):
        image = read_position(recorder, sequence)
        availability = position_availability(recorder, sequence)
        positions.append({
            'sequence': sequence,
            'availability': availability.value,
            'slot': None if image is None else image.slot,
            'raw': None if image is None else image.raw.hex(),
        })

    operations = []
    rows = recorder.connection.execute('''
        SELECT operation, sequence, state, slot, raw
        FROM operations
        WHERE sequence BETWEEN ? AND ?
        ORDER BY operation
    ''', (start, end)).fetchall()
    for row in rows:
        operations.append({
            'operation': row['operation'],
            'sequence': row['sequence'],
            'state': row['state'],
            'slot': row['slot'],
            'raw': None if row['raw'] is None else row['raw'].hex(),
        })
    return {'positions': positions, 'operations': operations}


def _load_snapshot(row: sqlite3.Row) -> dict[str, Any] | None:
    text = row['snapshot']
    if text is None:
        return None
    snapshot = json.loads(text)
    if not isinstance(snapshot, dict):
        raise ValueError('stored transfer snapshot is invalid')
    return snapshot


def _canonical_snapshot(snapshot: dict[str, Any]) -> str:
    return json.dumps(snapshot, ensure_ascii=False, separators=(',', ':'), sort_keys=True)


def _validate_snapshot(snapshot: object, start: int, end: int) -> None:
    if not isinstance(snapshot, dict):
        raise ValueError('candidate snapshot must be an object')
    positions = snapshot.get('positions')
    operations = snapshot.get('operations')
    if not isinstance(positions, list) or not isinstance(operations, list):
        raise ValueError('candidate snapshot lists are invalid')

    seen_sequences = set()
    for position in positions:
        if not isinstance(position, dict):
            raise ValueError('candidate position is invalid')
        sequence = position.get('sequence')
        if type(sequence) is not int or not start <= sequence <= end or sequence in seen_sequences:
            raise ValueError('candidate position sequence is invalid')
        seen_sequences.add(sequence)
        availability = position.get('availability')
        if availability not in {item.value for item in PositionAvailability}:
            raise ValueError('candidate position availability is invalid')
        slot = position.get('slot')
        if slot is not None and type(slot) is not int:
            raise ValueError('candidate slot is invalid')
        raw = position.get('raw')
        if raw is not None:
            if not isinstance(raw, str):
                raise ValueError('candidate raw packet is invalid')
            bytes.fromhex(raw)

    if seen_sequences != set(range(start, end + 1)):
        raise ValueError('candidate does not cover the complete transfer range')

    for operation in operations:
        if not isinstance(operation, dict):
            raise ValueError('candidate operation is invalid')
        require_name(operation.get('operation'), 'operation')
        sequence = operation.get('sequence')
        if type(sequence) is not int or not start <= sequence <= end:
            raise ValueError('candidate operation sequence is invalid')
        if operation.get('state') not in {item.value for item in RepairState if item is not RepairState.UNKNOWN}:
            raise ValueError('candidate operation state is invalid')
        slot = operation.get('slot')
        if slot is not None and type(slot) is not int:
            raise ValueError('candidate operation slot is invalid')
        raw = operation.get('raw')
        if raw is not None:
            if not isinstance(raw, str):
                raise ValueError('candidate operation raw packet is invalid')
            bytes.fromhex(raw)


def _snapshot_position(snapshot: dict[str, Any], sequence: int) -> dict[str, Any] | None:
    for position in snapshot.get('positions', ()):  # type: ignore[union-attr]
        if position.get('sequence') == sequence:
            return position
    return None


def _snapshot_operation(snapshot: dict[str, Any], operation: str) -> dict[str, Any] | None:
    for item in snapshot.get('operations', ()):  # type: ignore[union-attr]
        if item.get('operation') == operation:
            return item
    return None


def _snapshot_missing(snapshot: dict[str, Any], sequence: int) -> bool:
    position = _snapshot_position(snapshot, sequence)
    if position is None or position.get('availability') != PositionAvailability.COVERED.value:
        return False
    return not any(
        item.get('sequence') == sequence and item.get('state') == RepairState.ISSUED.value
        for item in snapshot.get('operations', ())  # type: ignore[union-attr]
    )


def _image_from_snapshot_item(item: dict[str, Any], device: str) -> SlotImage | None:
    raw = item.get('raw')
    slot = item.get('slot')
    sequence = item.get('sequence')
    if raw is None or slot is None:
        return None
    return SlotImage(device, sequence, slot, bytes.fromhex(raw))


def _outcome_for_snapshot(
    row: sqlite3.Row, sequence: int, operation: str,
) -> RepairOutcome:
    snapshot = _load_snapshot(row)
    if snapshot is None:
        return RepairOutcome(operation, RepairState.PENDING, sequence)

    recorded = _snapshot_operation(snapshot, operation)
    if recorded is not None and recorded.get('sequence') != sequence:
        raise ValueError('repair operation conflicts with its transferred position')

    if recorded is not None:
        state = RepairState(recorded['state'])
        if state is RepairState.ISSUED:
            image = _image_from_snapshot_item(recorded, row['device'])
            return RepairOutcome(operation, RepairState.ISSUED, sequence, image)
        if state is RepairState.NOT_ISSUED:
            return RepairOutcome(operation, RepairState.NOT_ISSUED, sequence)
        # A pending number at a still-readable cutoff position can be taken
        # offline by the station under its accepted transfer authority.

    position = _snapshot_position(snapshot, sequence)
    if position is not None and position.get('availability') == PositionAvailability.READABLE.value:
        image = _image_from_snapshot_item(position, row['device'])
        if image is not None:
            return RepairOutcome(operation, RepairState.ISSUED, sequence, image)

    if position is not None and position.get('availability') == PositionAvailability.COVERED.value:
        return RepairOutcome(operation, RepairState.NOT_ISSUED, sequence)
    return RepairOutcome(operation, RepairState.PENDING, sequence)


# ---------------------------------------------------------------------------
# Public transfer protocol
# ---------------------------------------------------------------------------

def prepare(db: sqlite3.Connection, shift_id: str, transfer_id: str) -> bytes:
    shift_id = require_name(shift_id, 'shift id')
    transfer_id = _validate_identity(transfer_id)
    initialize_station_schema(db)
    shift = lookup_shift(db, shift_id)
    if shift.end is None:
        raise ValueError('transfer requires a fixed shift boundary')

    with _transaction(db, 'station') as database:
        row = _station_row(database, transfer_id)
        binding = {
            'transfer_id': transfer_id,
            'shift_id': shift.shift_id,
            'device': shift.device,
            'start': shift.start,
            'end': shift.end,
        }
        if row is not None:
            if not _binding_matches(row, binding):
                raise ValueError('transfer id already binds different parameters')
        else:
            other = database.execute(
                'SELECT transfer_id FROM repair_transfers WHERE shift_id = ?',
                (shift_id,),
            ).fetchone()
            if other is not None:
                raise ValueError('shift already has a repair transfer')
            published = database.execute(
                'SELECT 1 FROM statements WHERE shift_id = ?', (shift_id,),
            ).fetchone()
            if published is not None:
                raise ValueError('shift is already published')
            database.execute('''
                INSERT INTO repair_transfers
                    (transfer_id, shift_id, device, start, "end", status)
                VALUES (?, ?, ?, ?, ?, 'pending')
            ''', (transfer_id, shift_id, shift.device, shift.start, shift.end))
        return _encode_message('recorder', 'boundary', binding)


def offer(recorder: DeviceRepository, request: bytes) -> bytes:
    if not isinstance(recorder, DeviceRepository):
        raise ValueError('offer requires a DeviceRepository endpoint')
    initialize_recorder_schema(recorder.connection)
    document = _parse_message(request, recorder, audience='recorder', kind='boundary')

    with _transaction(recorder, 'recorder') as database:
        row = _recorder_row(recorder, document['transfer_id'])
        if row is not None:
            _check_binding(row, document)
            return bytes(row['candidate'])

        same_shift = database.execute(
            'SELECT transfer_id FROM repair_transfers WHERE shift_id = ?',
            (document['shift_id'],),
        ).fetchone()
        if same_shift is not None:
            raise ValueError('shift already has a repair transfer')

        start, end = document['start'], document['end']
        for sequence in range(start, end + 1):
            if position_availability(recorder, sequence) is PositionAvailability.UNOBSERVED:
                raise ValueError('offer requires every position in the shift to be recorded')

        snapshot = _build_snapshot(recorder, start, end)
        binding = {field: document[field] for field in _BINDING_FIELDS}
        candidate = _encode_message('station', 'candidate', binding, {'snapshot': snapshot})
        database.execute('''
            INSERT INTO repair_transfers
                (transfer_id, shift_id, device, start, "end", status, snapshot, candidate)
            VALUES (?, ?, ?, ?, ?, 'pending', ?, ?)
        ''', (
            document['transfer_id'], document['shift_id'], document['device'],
            start, end, _canonical_snapshot(snapshot), candidate,
        ))
        return candidate


def accept(db: sqlite3.Connection, transfer_id: str) -> tuple[bytes, ...]:
    transfer_id = _validate_identity(transfer_id)
    kind = _endpoint_kind(db)
    if kind != 'station':
        raise ValueError('accept is a station operation')
    initialize_station_schema(db)

    with _transaction(db, 'station') as database:
        row = _station_row(database, transfer_id)
        if row is None:
            raise KeyError(transfer_id)

        if row['status'] == 'withdrawn':
            # A reverse request never rewrites the terminal outcome.  Return
            # the durable withdrawal acknowledgement so a retrying caller can
            # reconcile the recorder.
            return (_withdraw_ack_message(row),)

        if row['status'] == 'accepted':
            return (_accept_message(row),)

        database.execute(
            'UPDATE repair_transfers SET accept_intent = 1 WHERE transfer_id = ?',
            (transfer_id,),
        )
        if row['snapshot'] is not None:
            database.execute('''
                UPDATE repair_transfers SET status = 'accepted'
                WHERE transfer_id = ? AND status = 'pending'
            ''', (transfer_id,))
            row = _station_row(database, transfer_id)
        return (_accept_message(row),)


def withdraw(recorder: DeviceRepository, transfer_id: str) -> tuple[bytes, ...]:
    transfer_id = _validate_identity(transfer_id)
    if not isinstance(recorder, DeviceRepository):
        raise ValueError('withdraw is a recorder operation')
    initialize_recorder_schema(recorder.connection)

    with _transaction(recorder, 'recorder') as database:
        row = _recorder_row(recorder, transfer_id)
        if row is None:
            raise KeyError(transfer_id)

        if row['status'] == 'accepted':
            # The authoritative acceptance has already won.  Do not create a
            # fresh withdrawal message that could confuse a late courier.
            return ()

        database.execute(
            'UPDATE repair_transfers SET withdraw_intent = 1 WHERE transfer_id = ?',
            (transfer_id,),
        )
        return (_withdraw_message(row),)


def status(endpoint: Any, transfer_id: str) -> str:
    transfer_id = _validate_identity(transfer_id)
    kind = _endpoint_kind(endpoint)
    _initialize(endpoint, kind)
    row = _station_row(_connection(endpoint), transfer_id) if kind == 'station' \
        else _recorder_row(endpoint, transfer_id)
    return 'unknown' if row is None else str(row['status'])


def deliver(endpoint: Any, message: bytes) -> tuple[bytes, ...]:
    kind = _endpoint_kind(endpoint)
    _initialize(endpoint, kind)
    expected_audience = 'station' if kind == 'station' else 'recorder'
    document = _parse_message(message, endpoint, audience=expected_audience)
    message_kind = document['kind']

    # Convenience for a caller that treats every portable byte through deliver.
    if message_kind == 'boundary' and kind == 'recorder':
        return (offer(endpoint, message),)

    if kind == 'station':
        if message_kind == 'candidate':
            return _deliver_candidate(endpoint, document, message)
        if message_kind == 'withdraw':
            return _deliver_withdraw(endpoint, document)
        raise ValueError('station cannot accept this transfer message kind')

    if message_kind == 'accept':
        return _deliver_accept(endpoint, document)
    if message_kind == 'withdraw_ack':
        return _deliver_withdraw_ack(endpoint, document)
    raise ValueError('recorder cannot accept this transfer message kind')


def _deliver_candidate(
    db: sqlite3.Connection, document: dict[str, Any], raw_message: bytes,
) -> tuple[bytes, ...]:
    snapshot = document.get('data', {}).get('snapshot')
    _validate_snapshot(snapshot, document['start'], document['end'])

    with _transaction(db, 'station') as database:
        row = _station_row(database, document['transfer_id'])
        _check_binding(row, document)
        assert row is not None

        canonical = _canonical_snapshot(snapshot)
        if row['snapshot'] is not None and row['snapshot'] != canonical:
            raise ValueError('candidate conflicts with the stored cutoff evidence')
        if row['snapshot'] is None:
            database.execute('''
                UPDATE repair_transfers SET snapshot = ?, candidate = ?
                WHERE transfer_id = ?
            ''', (canonical, raw_message, document['transfer_id']))
            row = _station_row(database, document['transfer_id'])
        assert row is not None

        if row['status'] == 'pending' and row['accept_intent']:
            database.execute('''
                UPDATE repair_transfers SET status = 'accepted'
                WHERE transfer_id = ? AND status = 'pending'
            ''', (document['transfer_id'],))
        return ()


def _deliver_withdraw(db: sqlite3.Connection, document: dict[str, Any]) -> tuple[bytes, ...]:
    with _transaction(db, 'station') as database:
        row = _station_row(database, document['transfer_id'])
        _check_binding(row, document)
        assert row is not None

        if row['status'] == 'accepted':
            return (_accept_message(row),)
        if row['status'] == 'pending':
            database.execute('''
                UPDATE repair_transfers SET status = 'withdrawn'
                WHERE transfer_id = ? AND status = 'pending'
            ''', (document['transfer_id'],))
            row = _station_row(database, document['transfer_id'])
        return (_withdraw_ack_message(row),)


def _deliver_accept(
    recorder: DeviceRepository, document: dict[str, Any],
) -> tuple[bytes, ...]:
    with _transaction(recorder, 'recorder') as database:
        row = _recorder_row(recorder, document['transfer_id'])
        _check_binding(row, document)
        assert row is not None

        if row['status'] == 'pending':
            database.execute('''
                UPDATE repair_transfers SET status = 'accepted'
                WHERE transfer_id = ? AND status = 'pending'
            ''', (document['transfer_id'],))
            row = _recorder_row(recorder, document['transfer_id'])
        # An idempotent acceptance reply also carries the immutable candidate,
        # allowing a lost original candidate to be recovered.
        return (bytes(row['candidate']),)


def _deliver_withdraw_ack(
    recorder: DeviceRepository, document: dict[str, Any],
) -> tuple[bytes, ...]:
    with _transaction(recorder, 'recorder') as database:
        row = _recorder_row(recorder, document['transfer_id'])
        _check_binding(row, document)
        assert row is not None
        if row['status'] == 'pending':
            database.execute('''
                UPDATE repair_transfers SET status = 'withdrawn'
                WHERE transfer_id = ? AND status = 'pending'
            ''', (document['transfer_id'],))
        return ()


def repair(
    db: sqlite3.Connection, transfer_id: str, sequence: int, operation: str,
) -> RepairOutcome:
    transfer_id = _validate_identity(transfer_id)
    operation = require_name(operation, 'operation')
    require_integer(sequence, 'sequence')
    if _endpoint_kind(db) != 'station':
        raise ValueError('repair requires a station connection')
    initialize_station_schema(db)

    with _transaction(db, 'station') as database:
        row = _station_row(database, transfer_id)
        if row is None or row['status'] != 'accepted' or row['snapshot'] is None:
            raise ValueError('repair transfer has not taken effect')
        if not row['start'] <= sequence <= row['end']:
            raise ValueError('repair position is outside the transfer range')

        repository = CaptureRepository(database)
        request = repository.get_request(operation)
        bound = StoredRepairRequest(
            operation, row['shift_id'], row['device'], sequence,
        )
        if request is None:
            repository.insert_request(bound)
        elif request != bound:
            raise ValueError('repair operation conflicts with its registered position')

        return _outcome_for_snapshot(row, sequence, operation)


# ---------------------------------------------------------------------------
# Hooks used by the existing station/device entry points
# ---------------------------------------------------------------------------

def assert_ordinary_request_allowed(database: sqlite3.Connection, shift) -> None:
    row = database.execute('''
        SELECT status FROM repair_transfers
        WHERE shift_id = ? AND status IN ('pending', 'accepted')
        LIMIT 1
    ''', (shift.shift_id,)).fetchone()
    if row is not None:
        raise ValueError('ordinary repair registration is suspended for the transfer')


def assert_direct_intake_allowed(database: sqlite3.Connection, image: SlotImage) -> None:
    # Exact redelivery of an existing formal receipt remains idempotent.
    if CaptureRepository(database).get_receipt(image.device, image.sequence) is not None:
        return
    row = database.execute('''
        SELECT 1 FROM repair_transfers
        WHERE device = ? AND start <= ? AND "end" >= ?
          AND status IN ('pending', 'accepted')
        LIMIT 1
    ''', (image.device, image.sequence, image.sequence)).fetchone()
    if row is not None:
        raise ValueError('direct intake is closed for a transferred position')


def authorize_offline_receive(
    database: sqlite3.Connection, request: StoredRepairRequest, image: SlotImage,
) -> StoredRepairRequest:
    row = database.execute('''
        SELECT * FROM repair_transfers
        WHERE shift_id = ? AND device = ? AND status = 'accepted'
        LIMIT 1
    ''', (request.shift_id, request.device)).fetchone()
    if row is None or row['snapshot'] is None:
        raise ValueError('repair reply requires its source device')
    outcome = _outcome_for_snapshot(row, image.sequence, request.operation)
    if outcome.state is not RepairState.ISSUED or outcome.image != image:
        raise ValueError('repair reply does not match the issued original')
    return request


def transfer_for_shift(database: sqlite3.Connection, shift_id: str) -> sqlite3.Row | None:
    return database.execute(
        'SELECT * FROM repair_transfers WHERE shift_id = ? ORDER BY rowid LIMIT 1',
        (shift_id,),
    ).fetchone()


def resolve_transfer_coverage(database: sqlite3.Connection, shift) -> tuple:
    from station.closeout.coverage import PositionOutcome

    row = transfer_for_shift(database, shift.shift_id)
    if row is None or row['status'] != 'accepted':
        return tuple(
            PositionOutcome(sequence, 'waiting')
            for sequence in range(shift.start, shift.end + 1)
        )

    snapshot = _load_snapshot(row)
    received = {
        receipt.packet.sequence: receipt
        for receipt in receipts_for_range(database, shift.device, shift.start, shift.end)
    }
    outcomes = []
    for sequence in range(shift.start, shift.end + 1):
        receipt = received.get(sequence)
        if receipt is not None:
            outcomes.append(PositionOutcome(sequence, 'received', receipt))
        elif snapshot is not None and _snapshot_missing(snapshot, sequence):
            outcomes.append(PositionOutcome(sequence, 'missing'))
        else:
            outcomes.append(PositionOutcome(sequence, 'waiting'))
    return tuple(outcomes)


def device_operation_blocked(repository: DeviceRepository, operation: str) -> bool:
    """Called inside an existing device transaction."""
    row = repository.connection.execute(
        'SELECT sequence FROM operations WHERE operation = ?', (operation,),
    ).fetchone()
    if row is None:
        return False
    transfer = repository.connection.execute('''
        SELECT 1 FROM repair_transfers
        WHERE device = ? AND start <= ? AND "end" >= ?
          AND status IN ('pending', 'accepted')
        LIMIT 1
    ''', (repository.device, row['sequence'], row['sequence'])).fetchone()
    return transfer is not None
