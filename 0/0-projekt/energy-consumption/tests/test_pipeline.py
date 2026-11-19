import csv
import json
import shutil
import sqlite3
import unittest
from contextlib import ExitStack, closing, redirect_stdout
from io import StringIO
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from src.pipeline import run, transform

INPUT = Path(__file__).resolve().parents[1] / 'data' / 'input.csv'


class PipelineTests(unittest.TestCase):
    def setUp(self):
        stack = ExitStack()
        self.addCleanup(stack.close)
        base = Path(stack.enter_context(TemporaryDirectory()))
        self.input = base / 'data' / 'input.csv'
        self.input.parent.mkdir()
        self.output = base / 'output'
        stack.enter_context(patch('src.pipeline.BASE', base))
        stack.enter_context(redirect_stdout(StringIO()))

    def test_valid_row(self):
        with INPUT.open(newline='', encoding='utf-8') as handle:
            row = next(csv.DictReader(handle))
        record = transform(row)
        self.assertEqual(record['value'], 8.2)
        self.assertEqual(record['flag'], 0)

    def test_pipeline(self):
        shutil.copyfile(INPUT, self.input)
        self.assertEqual(run(), [
            {'group': 'M1', 'records': 2, 'total_value': 19.5, 'flags': 0},
            {'group': 'M2', 'records': 1, 'total_value': 42.5, 'flags': 1},
        ])

    def test_pipeline_rejects_invalid_rows(self):
        self.input.write_text(
            'meter_id,consumption_kwh\n'
            'M1,30\n'
            'M1,31\n'
            ',20\n'
            ' ,20\n'
            'M2,NaN\n'
            'M2,Infinity\n'
            'M2,-1\n'
            'M2,45,extra\n',
            encoding='utf-8',
        )
        self.assertEqual(run(), [{'group': 'M1', 'records': 2, 'total_value': 61.0, 'flags': 1}])
        rejected = json.loads((self.output / 'rejected.json').read_text(encoding='utf-8'))
        self.assertEqual([entry['reason'] for entry in rejected], [
            'Invalid row: missing meter_id',
            'Invalid row: missing meter_id',
            'Invalid row: non-finite consumption',
            'Invalid row: non-finite consumption',
            'Invalid row: negative consumption',
            'Invalid row: extra CSV fields',
        ])

    def test_overflow_preserves_previous_results(self):
        shutil.copyfile(INPUT, self.input)
        previous_summary = run()
        self.input.write_text('meter_id,consumption_kwh\nM1,1e308\nM1,1e308\n', encoding='utf-8')

        with self.assertRaisesRegex(RuntimeError, 'Consumption total exceeds'):
            run()

        self.assertEqual(json.loads((self.output / 'summary.json').read_text(encoding='utf-8')), previous_summary)
        with closing(sqlite3.connect(self.output / 'warehouse.db')) as con:
            self.assertEqual(con.execute('SELECT COUNT(*) FROM events').fetchone(), (3,))


if __name__ == '__main__': unittest.main()
