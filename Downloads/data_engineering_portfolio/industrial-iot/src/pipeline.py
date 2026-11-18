"""Small dependency-free ETL example. Input is synthetic demonstration data."""
import csv
import json
import sqlite3
from math import isfinite
from pathlib import Path

BASE = Path(__file__).resolve().parents[1]
DOMAIN = "iot"
ID_FIELDS = ('event_id', 'machine_id')
MEASUREMENT_FIELDS = ('temperature_c', 'vibration_mm_s', 'pressure_bar')

def transform(row):
    """Return normalized record or raise ValueError for invalid source rows."""
    out = dict(row)
    try:
        if DOMAIN == 'retail':
            qty, price, stock = int(row['quantity']), float(row['unit_price']), int(row['stock'])
            if min(qty, price, stock) < 0: raise ValueError('negative retail value')
            out['value'] = round(qty * price, 2)
            out['flag'] = int(stock < 15)
        elif DOMAIN == 'iot':
            if None in row:
                raise ValueError('row has more fields than the CSV header')
            for field in ID_FIELDS:
                identifier = row.get(field)
                if not isinstance(identifier, str) or not identifier.strip():
                    raise ValueError(f'missing {field}')
            temp, vibration, pressure = (float(row[field]) for field in MEASUREMENT_FIELDS)
            if not all(map(isfinite, (temp, vibration, pressure))):
                raise ValueError('measurements must be finite')
            if pressure < 0:
                raise ValueError('negative pressure')
            out['value'] = 1.0
            out['flag'] = int(temp > 85 or vibration > 6)
        elif DOMAIN == 'banking':
            amount, hour = float(row['amount']), int(row['hour'])
            if amount < 0 or not 0 <= hour <= 23: raise ValueError('invalid transaction')
            out['value'] = amount
            out['flag'] = int(amount >= 10000 or hour < 5)
        elif DOMAIN == 'energy':
            consumption = float(row['consumption_kwh'])
            if consumption < 0: raise ValueError('negative consumption')
            out['value'] = consumption
            out['flag'] = int(consumption > 30)
        elif DOMAIN == 'healthcare':
            tests = int(row['lab_tests'])
            if tests < 0 or row['status'] not in ('completed', 'cancelled', 'scheduled'): raise ValueError('invalid appointment')
            out['value'] = 1.0
            out['flag'] = int(row['status'] == 'cancelled')
        else: raise ValueError('unknown domain')
    except (KeyError, TypeError, ValueError) as exc:
        raise ValueError(f'Invalid row: {exc}') from exc
    return out

def run():
    input_path = BASE / 'data' / 'input.csv'
    output = BASE / 'output'
    valid, rejected = [], []
    with input_path.open(newline='', encoding='utf-8-sig') as handle:
        reader = csv.DictReader(handle)
        if DOMAIN == 'iot':
            fieldnames = reader.fieldnames or []
            missing = [field for field in (*ID_FIELDS, *MEASUREMENT_FIELDS) if field not in fieldnames]
            if missing:
                raise ValueError(f'Missing required CSV columns: {", ".join(missing)}')
            if len(set(fieldnames)) != len(fieldnames):
                raise ValueError('CSV header contains duplicate columns')
        for row in reader:
            try:
                valid.append(transform(row))
            except ValueError as exc:
                rejected.append({'row': row, 'reason': str(exc)})

    output.mkdir(exist_ok=True)
    con = sqlite3.connect(output / 'warehouse.db')
    try:
        with con:
            con.execute('BEGIN')
            con.execute('DROP TABLE IF EXISTS events')
            con.execute('CREATE TABLE events (group_key TEXT NOT NULL, value REAL NOT NULL, flag INTEGER NOT NULL)')
            con.executemany(
                'INSERT INTO events VALUES (?, ?, ?)',
                [(row['machine_id'], row['value'], row['flag']) for row in valid],
            )
            result = con.execute('''
                SELECT group_key, COUNT(*), ROUND(SUM(value), 2), SUM(flag)
                FROM events
                GROUP BY group_key
                ORDER BY group_key
            ''').fetchall()
    finally:
        con.close()

    summary = [
        {'group': group, 'records': records, 'total_value': value, 'flags': flags}
        for group, records, value, flags in result
    ]
    with (output / 'clean.jsonl').open('w', encoding='utf-8') as handle:
        for row in valid:
            handle.write(json.dumps(row) + '\n')
    (output / 'rejected.json').write_text(json.dumps(rejected, indent=2), encoding='utf-8')
    (output / 'summary.json').write_text(json.dumps(summary, indent=2), encoding='utf-8')
    print(json.dumps({'accepted': len(valid), 'rejected': len(rejected), 'summary': summary}, indent=2))
    return summary

if __name__ == '__main__':
    run()
