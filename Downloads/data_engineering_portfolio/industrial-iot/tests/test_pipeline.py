import json
import sqlite3
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

from src import pipeline


DATA_FILE = Path(__file__).resolve().parents[1] / 'data' / 'input.csv'
HEADER = 'event_id,machine_id,temperature_c,vibration_mm_s,pressure_bar\n'

class PipelineTests(unittest.TestCase):
    def setUp(self):
        directory = TemporaryDirectory()
        self.addCleanup(directory.cleanup)
        base = Path(directory.name)
        self.input_file = base / 'data' / 'input.csv'
        self.input_file.parent.mkdir()
        self.output = base / 'output'
        base_patch = patch.object(pipeline, 'BASE', base)
        base_patch.start()
        self.addCleanup(base_patch.stop)

    def test_pipeline(self):
        self.input_file.write_bytes(DATA_FILE.read_bytes())
        summary = pipeline.run()
        self.assertEqual(summary, [
            {'group': 'M1', 'records': 2, 'total_value': 2.0, 'flags': 0},
            {'group': 'M2', 'records': 1, 'total_value': 1.0, 'flags': 1},
        ])
        clean = [json.loads(line) for line in (self.output / 'clean.jsonl').read_text().splitlines()]
        self.assertEqual([(row['event_id'], row['flag']) for row in clean], [('E1', 0), ('E2', 1), ('E3', 0)])
        rejected = json.loads((self.output / 'rejected.json').read_text())
        self.assertEqual([entry['row']['event_id'] for entry in rejected], ['E4'])

    def test_all_invalid_rows_replace_previous_results(self):
        self.input_file.write_text(HEADER + 'E1,M1,85,6,5\n', encoding='utf-8')
        self.assertEqual(pipeline.run()[0]['flags'], 0)
        self.input_file.write_text(
            HEADER + 'E2,M1,NaN,1,5\nE3,M1,20,inf,5\nE4, ,20,1,5\nE5,M1,20,1,5,extra\n',
            encoding='utf-8',
        )
        self.assertEqual(pipeline.run(), [])
        self.assertEqual((self.output / 'clean.jsonl').read_text(), '')
        self.assertEqual(json.loads((self.output / 'summary.json').read_text()), [])
        rejected = json.loads((self.output / 'rejected.json').read_text())
        self.assertEqual([entry['row']['event_id'] for entry in rejected], ['E2', 'E3', 'E4', 'E5'])
        self.assertIn('missing machine_id', rejected[2]['reason'])
        connection = sqlite3.connect(self.output / 'warehouse.db')
        try:
            self.assertEqual(connection.execute('SELECT COUNT(*) FROM events').fetchone(), (0,))
        finally:
            connection.close()

    def test_missing_required_csv_column(self):
        self.input_file.write_text('event_id,temperature_c,vibration_mm_s,pressure_bar\n', encoding='utf-8')
        with self.assertRaisesRegex(ValueError, 'Missing required CSV columns: machine_id'):
            pipeline.run()
        self.assertFalse(self.output.exists())


if __name__ == '__main__':
    unittest.main()
