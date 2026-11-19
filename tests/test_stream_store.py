import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace

from postgres_support import POSTGRES_TEST_DB, use_test_database


@unittest.skipUnless(POSTGRES_TEST_DB, 'Set ENERGY_TEST_PG_DATABASE to enable PostgreSQL tests')
class StreamStoreTests(unittest.TestCase):
    def setUp(self):
        from src.stream_store import initialize

        use_test_database(self)
        self.warehouse_id = initialize()

    def reading(self, offset, reason=None):
        return SimpleNamespace(
            topic='readings', partition=0, offset=offset, reading_id='R1',
            meter_id='M1', reading_date='2026-01-01', consumption_kwh=31.0,
            high_consumption=1, reason=reason, payload='{}',
        )

    def test_replay_preserves_counts_and_distinguishes_duplicates(self):
        from src.quality import report
        from src.stream_store import connect, persist_batch

        rows = [self.reading(0, 'invalid_json'), self.reading(1), self.reading(2)]
        persist_batch(rows, 0)
        persist_batch(rows, 0)

        metrics = report(10)
        self.assertEqual(len(metrics['batches']), 1)
        self.assertEqual((metrics['accepted'], metrics['rejected'], metrics['duplicates']), (1, 1, 1))
        self.assertAlmostEqual(metrics['reject_rate'], 1 / 3)
        self.assertAlmostEqual(metrics['duplicate_rate'], 1 / 3)
        with connect() as con:
            reasons = con.execute('SELECT rejection_reason FROM quarantine ORDER BY kafka_offset').fetchall()
        self.assertEqual(reasons, [('invalid_json',), ('duplicate_reading_id',)])

    def test_failed_batch_rolls_back_before_retry(self):
        from src.quality import report
        from src.stream_store import persist_batch

        def failed_rows():
            yield self.reading(0)
            raise RuntimeError('interrupted batch')

        with self.assertRaisesRegex(RuntimeError, 'interrupted batch'):
            persist_batch(failed_rows(), 0)
        self.assertEqual(report(1)['total'], 0)
        persist_batch([self.reading(0)], 0)
        self.assertEqual(report(1)['accepted'], 1)

    def test_quality_lookback_excludes_old_batches(self):
        from src.quality import report
        from src.stream_store import connect, persist_batch

        persist_batch([self.reading(0, 'invalid_json')], 0)
        with connect() as con:
            con.execute("UPDATE public.quality_batches SET recorded_at = now() - interval '25 hours'")

        self.assertEqual(report(lookback_hours=24)['total'], 0)
        persist_batch([self.reading(1)], 1)
        persist_batch([self.reading(2)], 2)

        recent = report(lookback_hours=24)
        self.assertEqual((recent['accepted'], recent['rejected'], recent['duplicates']), (1, 0, 1))
        self.assertEqual(recent['duplicate_rate'], 0.5)
        self.assertEqual(report(batches=1, lookback_hours=24)['total'], 1)

    def test_checkpoint_rejects_a_different_warehouse(self):
        from src.stream_store import bind_checkpoint

        with TemporaryDirectory() as directory:
            state = Path(directory)
            self.assertEqual(bind_checkpoint(state, self.warehouse_id), state / 'checkpoint')
            with self.assertRaisesRegex(RuntimeError, 'does not match'):
                bind_checkpoint(state, 'different-warehouse')
