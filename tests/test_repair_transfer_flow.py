import unittest
from contextlib import closing
from pathlib import Path
from tempfile import TemporaryDirectory

import station.repair_transfer as repair_transfer
from station import open_station
from station.capture import receive, request_repair
from station.closeout import close_shift
from station.device import DeviceRepository, issue_repair
from station.device.operations import RepairState
from station.review import retrieve
from transfer_scene import DEVICE, SHIFT, TRANSFER, Courier, advance, seed, verify_v1


class RepairTransferFlowTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.station_path = self.root / 'station.sqlite'
        self.recorder_path = self.root / 'recorder.sqlite'
        self.db = open_station(self.station_path)
        self.recorder = DeviceRepository(self.recorder_path, DEVICE, 2)
        self.addCleanup(self.db.close)
        self.addCleanup(self.recorder.close)
        self.r41_image = seed(self.db, self.recorder)

    def reopen(self):
        self.db.close()
        self.recorder.close()
        self.db = open_station(self.station_path)
        self.recorder = DeviceRepository(self.recorder_path, DEVICE, 2)
        self.addCleanup(self.db.close)
        self.addCleanup(self.recorder.close)

    def assert_takeover_statement(self, publication):
        statement = publication.statement
        self.assertEqual(
            [(row.sequence, row.state) for row in statement.positions],
            [(41, 'received'), (42, 'received'), (43, 'missing')],
        )
        r41, r42, r43 = statement.positions
        self.assertEqual((r41.receipt.packet.device, r41.receipt.packet.slot), (DEVICE, 0))
        self.assertEqual((r41.receipt.packet.sequence, r41.receipt.packet.micrometers), (41, 7))
        self.assertEqual(r41.receipt.operation, 'R41')
        self.assertEqual((r42.receipt.packet.slot, r42.receipt.packet.micrometers), (1, 11))
        self.assertEqual(r42.receipt.operation, 'R42')
        self.assertIsNone(r43.receipt)
        self.assertEqual(
            statement.stats,
            publication.statement.stats.__class__(2, 7, 11, (43,)),
        )
        self.assertEqual(publication.first_receipt.receipt_id, 'first/shift/outgoing')

    def test_accepted_terminal_survives_reopen_duplicates_and_late_withdraw(self):
        courier = Courier()
        request = repair_transfer.prepare(self.db, SHIFT, TRANSFER)
        self.assertIsInstance(request, bytes)
        candidate = repair_transfer.offer(self.recorder, request)
        self.assertIsInstance(candidate, bytes)

        accept_messages = repair_transfer.accept(self.db, TRANSFER)
        self.assertEqual(len(accept_messages), 1)
        late_withdraw = repair_transfer.withdraw(self.recorder, TRANSFER)
        self.assertEqual(len(late_withdraw), 1)
        courier.hold('late-withdraw', *late_withdraw)

        # While the decision is unresolved, old registration/signing/intake
        # entries cannot bypass the handover.
        self.assertEqual(repair_transfer.status(self.db, TRANSFER), 'pending')
        self.assertEqual(repair_transfer.status(self.recorder, TRANSFER), 'pending')
        with self.assertRaises(ValueError):
            request_repair(self.db, SHIFT, DEVICE, 42, 'bypass-number')
        waiting = close_shift(self.db, SHIFT)
        self.assertEqual(waiting.status, 'waiting')
        self.assertIsNone(waiting.publication)
        self.assertEqual(waiting.pending, (41, 42, 43))
        with self.assertRaises(ValueError):
            receive(self.db, self.r41_image)
        # An already handed-over repair packet can still arrive through its
        # original sourced operation path while the decision is pending.
        online_r41 = receive(
            self.db, self.r41_image, operation='R41', source=self.recorder,
        )
        self.assertEqual(online_r41.operation, 'R41')
        self.assertEqual(close_shift(self.db, SHIFT).pending, (42, 43))
        with self.assertRaises(ValueError):
            repair_transfer.repair(self.db, TRANSFER, 41, 'R41')

        # The old named entry and replay events stay pending without releasing
        # R42; sequence 45 outside the shift can still cover slot 1.
        self.assertEqual(issue_repair(self.recorder, 'R42').state, RepairState.PENDING)
        issue_event = advance(self.recorder)
        reply_event = advance(self.recorder)
        cover_event = advance(self.recorder)
        self.assertEqual(issue_event.repair.state, RepairState.PENDING)
        self.assertIsNone(issue_event.image)
        self.assertEqual(reply_event.repair.state, RepairState.PENDING)
        self.assertIsNone(reply_event.image)
        self.assertEqual(cover_event.covered_sequence, 42)

        replies = repair_transfer.deliver(self.recorder, accept_messages[0])
        self.assertTrue(replies and all(isinstance(item, bytes) for item in replies))
        self.assertEqual(repair_transfer.status(self.recorder, TRANSFER), 'accepted')
        self.assertEqual(repair_transfer.deliver(self.db, candidate), ())
        self.assertEqual(repair_transfer.status(self.db, TRANSFER), 'accepted')

        # A late withdrawal cannot reverse the accepted terminal.
        late_replies = courier.deliver('late-withdraw', self.db, repair_transfer.deliver)
        self.assertEqual(len(late_replies), 1)
        repair_transfer.deliver(self.recorder, late_replies[0])
        self.assertEqual(repair_transfer.status(self.db, TRANSFER), 'accepted')
        self.assertEqual(repair_transfer.status(self.recorder, TRANSFER), 'accepted')
        self.assertEqual(issue_repair(self.recorder, 'R42').state, RepairState.PENDING)

        r41_outcome = repair_transfer.repair(self.db, TRANSFER, 41, 'R41')
        r42_outcome = repair_transfer.repair(self.db, TRANSFER, 42, 'R42')
        r43_outcome = repair_transfer.repair(self.db, TRANSFER, 43, 'R43')
        self.assertEqual(r41_outcome.state, RepairState.ISSUED)
        self.assertEqual(r41_outcome.image, self.r41_image)
        self.assertEqual(r42_outcome.state, RepairState.ISSUED)
        self.assertEqual((r42_outcome.image.slot, r42_outcome.image.sequence), (1, 42))
        self.assertEqual(r43_outcome.state, RepairState.NOT_ISSUED)

        self.assertEqual(receive(self.db, r41_outcome.image, operation='R41').operation, 'R41')
        self.assertEqual(receive(self.db, r42_outcome.image, operation='R42').operation, 'R42')
        signed = close_shift(self.db, SHIFT)
        self.assertEqual(signed.status, 'published')
        publication = signed.publication
        self.assert_takeover_statement(publication)

        # Closing the recorder proves the station result is source-independent.
        self.recorder.close()
        self.reopen()
        frozen = retrieve(self.db, 'shift/outgoing')
        self.assertEqual(frozen, publication)
        self.assert_takeover_statement(frozen)

        # Repeated/reopened requests and messages preserve the same terminal.
        self.assertEqual(repair_transfer.prepare(self.db, SHIFT, TRANSFER), request)
        self.assertEqual(repair_transfer.status(self.db, TRANSFER), 'accepted')
        self.assertEqual(repair_transfer.deliver(self.db, candidate), ())
        with closing(DeviceRepository(self.recorder_path, DEVICE, 2)) as recorder:
            self.assertEqual(repair_transfer.offer(recorder, request), candidate)
            self.assertEqual(repair_transfer.status(recorder, TRANSFER), 'accepted')
        with self.assertRaises(ValueError):
            repair_transfer.deliver(self.db, b'not a transfer message')
        self.assertEqual(repair_transfer.status(self.db, TRANSFER), 'accepted')

    def test_withdrawn_terminal_reopens_original_flow_and_rejects_late_candidate(self):
        request = repair_transfer.prepare(self.db, SHIFT, TRANSFER)
        candidate = repair_transfer.offer(self.recorder, request)
        withdraw_messages = repair_transfer.withdraw(self.recorder, TRANSFER)
        self.assertEqual(len(withdraw_messages), 1)
        self.assertEqual(repair_transfer.status(self.recorder, TRANSFER), 'pending')

        ack_messages = repair_transfer.deliver(self.db, withdraw_messages[0])
        self.assertEqual(len(ack_messages), 1)
        self.assertEqual(repair_transfer.status(self.db, TRANSFER), 'withdrawn')

        # Withhold the acknowledgement across a full close/reopen.  Retrying
        # withdraw reconstructs the same request and acknowledgement.
        self.reopen()
        self.assertEqual(repair_transfer.status(self.recorder, TRANSFER), 'pending')
        retried_withdraw = repair_transfer.withdraw(self.recorder, TRANSFER)
        self.assertEqual(len(retried_withdraw), 1)
        retried_ack = repair_transfer.deliver(self.db, retried_withdraw[0])
        self.assertEqual(len(retried_ack), 1)
        self.assertEqual(repair_transfer.deliver(self.recorder, retried_ack[0]), ())
        # The original delayed acknowledgement is idempotent.
        self.assertEqual(repair_transfer.deliver(self.recorder, ack_messages[0]), ())
        self.assertEqual(repair_transfer.status(self.recorder, TRANSFER), 'withdrawn')
        self.assertEqual(repair_transfer.status(self.db, TRANSFER), 'withdrawn')

        issued_r42 = issue_repair(self.recorder, 'R42')
        self.assertEqual(issued_r42.state, RepairState.ISSUED)
        self.assertEqual((issued_r42.sequence, issued_r42.image.slot), (42, 1))

        # The recording cursor is still at its issue event; named issue first
        # lets both replay events recover the same original.
        self.assertEqual(advance(self.recorder).repair, issued_r42)
        reply = advance(self.recorder)
        self.assertEqual(reply.image, issued_r42.image)
        self.assertEqual(advance(self.recorder).covered_sequence, 42)

        self.assertEqual(
            request_repair(self.db, SHIFT, DEVICE, 42, 'R42', source=self.recorder).operation,
            'R42',
        )
        received_r42 = receive(
            self.db, issued_r42.image, operation='R42', source=self.recorder,
        )
        received_r41 = receive(
            self.db, self.r41_image, operation='R41', source=self.recorder,
        )
        self.assertEqual((received_r41.packet.sequence, received_r42.packet.sequence), (41, 42))

        # A late candidate cannot become offline takeover authority.
        self.assertEqual(repair_transfer.deliver(self.db, candidate), ())
        self.assertEqual(repair_transfer.status(self.db, TRANSFER), 'withdrawn')
        with self.assertRaises(ValueError):
            repair_transfer.repair(self.db, TRANSFER, 42, 'R42')
        with self.assertRaisesRegex(ValueError, 'source device'):
            receive(self.db, issued_r42.image, operation='R42')

        request_repair(self.db, SHIFT, DEVICE, 43, 'R43', source=self.recorder)
        self.assertEqual(issue_repair(self.recorder, 'R43').state, RepairState.NOT_ISSUED)
        signed = close_shift(self.db, SHIFT, source=self.recorder)
        self.assertEqual(signed.status, 'published')
        self.assertEqual(
            signed.publication.statement.stats,
            signed.publication.statement.stats.__class__(2, 7, 11, (43,)),
        )

        self.reopen()
        self.assertEqual(retrieve(self.db, 'shift/outgoing'), signed.publication)


class TransferSceneRegressionTests(unittest.TestCase):
    def test_ordinary_fixture_survives_reopen(self):
        with TemporaryDirectory() as directory:
            root = Path(directory)
            result = verify_v1(root / 'station.sqlite', root / 'recorder.sqlite', reopen=True)
        self.assertTrue(result['reopened'])
        self.assertEqual(result['at_offer_availability'], ['covered', 'readable', 'covered'])
        self.assertEqual(result['ordinary_offline_named_repair'], 'rejected')
        self.assertEqual(result['ordinary_offline_pending'], [43])


if __name__ == '__main__':
    unittest.main()
