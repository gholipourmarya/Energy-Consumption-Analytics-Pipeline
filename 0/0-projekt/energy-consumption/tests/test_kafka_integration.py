import json
import os
import subprocess
import sys
import unittest
from datetime import date
from decimal import Decimal
from pathlib import Path
from tempfile import TemporaryDirectory
from uuid import uuid4

from postgres_support import POSTGRES_TEST_DB, use_test_database

BASE = Path(__file__).resolve().parents[1]
BROKER = os.environ.get('ENERGY_TEST_KAFKA_BROKER')

INTERRUPT_AFTER_COMMIT = '''
from src import stream

persist_batch = stream.persist_batch

def interrupt(rows, batch_id):
    persist_batch(rows, batch_id)
    raise RuntimeError('simulated interruption after PostgreSQL commit')

stream.persist_batch = interrupt
stream.main()
'''


@unittest.skipUnless(BROKER and POSTGRES_TEST_DB, 'Set Kafka and PostgreSQL test environment variables')
class KafkaIntegrationTests(unittest.TestCase):
    def test_incremental_checkpoint_and_quality(self):
        from kafka.admin import KafkaAdminClient, NewTopic
        from src.quality import report
        from src.stream_store import connect

        use_test_database(self)

        topic = f'energy-test-{uuid4().hex}'
        admin = KafkaAdminClient(bootstrap_servers=BROKER, request_timeout_ms=15_000)
        self.addCleanup(admin.close)
        admin.create_topics([NewTopic(topic, num_partitions=1, replication_factor=1)])
        self.addCleanup(admin.delete_topics, [topic])

        temporary = TemporaryDirectory()
        self.addCleanup(temporary.cleanup)
        state = Path(temporary.name)

        self.run_module('publish_readings', 'data/input.csv', '--broker', BROKER, '--topic', topic)
        stream_args = ('--broker', BROKER, '--topic', topic, '--state-dir', str(state), '--max-offsets', '2')
        interrupted = self.run_command(sys.executable, '-c', INTERRUPT_AFTER_COMMIT, *stream_args, expected=1)
        self.assertIn('stream_batch_failed', interrupted.stderr)
        self.assertIn('simulated interruption after PostgreSQL commit', interrupted.stderr)
        self.assertEqual(report(10)['total'], 2)
        self.assertFalse((state / 'checkpoint' / 'commits' / '0').exists())

        recovered = self.run_module('stream', *stream_args)
        self.assertIn('batch_id=0 accepted=0 rejected=0 duplicates=0 replayed=2', recovered.stderr)
        first = report(10)
        self.assertEqual((first['accepted'], first['rejected'], first['duplicates']), (3, 1, 0))
        self.assertEqual(len(first['batches']), 2)

        more = state / 'more.csv'
        more.write_text(
            'reading_id,meter_id,date,consumption_kwh\n'
            'R3,M2,2026-01-01,18\n'
            'R5,M1,2026-01-03,30\n'
            'R6,M1,2026-02-30,2\n'
            'R7,M1,2026-01-03,NaN\n'
            'R8,M1,2026-01-03,Infinity\n',
            encoding='utf-8',
        )
        self.run_module('publish_readings', str(more), '--broker', BROKER, '--topic', topic)
        self.run_module('stream', *stream_args)
        latest = report(10)
        self.assertEqual((latest['accepted'], latest['rejected'], latest['duplicates']), (4, 4, 1))
        self.assertGreater(latest['batches'][2]['spark_batch_id'], first['batches'][-1]['spark_batch_id'])
        self.run_command(
            str(Path(sys.executable).parent / 'dbt'), 'build', '--project-dir', 'dbt',
            '--profiles-dir', 'dbt', '--target-path', str(state / 'dbt-target'),
            '--log-path', str(state / 'dbt-logs'),
        )
        with connect() as con:
            summary = con.execute((BASE / 'sql' / 'stream_analysis.sql').read_text(encoding='utf-8')).fetchall()
            daily_totals = con.execute(
                'SELECT reading_date, total_kwh, flagged_readings '
                'FROM analytics.daily_consumption ORDER BY reading_date'
            ).fetchall()
            rejected = con.execute('SELECT rejection_reason FROM quarantine ORDER BY kafka_offset').fetchall()
        self.assertEqual(summary, [
            ('M1', date(2026, 1, 1), 1, Decimal('8.20'), 0),
            ('M2', date(2026, 1, 1), 1, Decimal('42.50'), 1),
            ('M1', date(2026, 1, 2), 1, Decimal('11.30'), 0),
            ('M1', date(2026, 1, 3), 1, Decimal('30.00'), 0),
        ])
        self.assertEqual(daily_totals, [
            (date(2026, 1, 1), Decimal('50.7'), 1),
            (date(2026, 1, 2), Decimal('11.3'), 0),
            (date(2026, 1, 3), Decimal('30'), 0),
        ])
        self.assertEqual(rejected, [
            ('negative_consumption',), ('duplicate_reading_id',), ('invalid_date',),
            ('invalid_consumption',), ('invalid_consumption',),
        ])

        self.run_module('stream', *stream_args)
        self.assertEqual(report(10), latest)

        quality = self.run_module('quality', '--batches', '10', expected=1)
        self.assertEqual(json.loads(quality.stdout)['alerts'], ['reject_rate_exceeded'])

    def run_module(self, name, *args, expected=0):
        return self.run_command(sys.executable, '-m', f'src.{name}', *args, expected=expected)

    def run_command(self, *args, expected=0):
        result = subprocess.run(
            args,
            cwd=BASE, capture_output=True, text=True, timeout=240,
        )
        self.assertEqual(result.returncode, expected, result.stdout + result.stderr)
        return result
