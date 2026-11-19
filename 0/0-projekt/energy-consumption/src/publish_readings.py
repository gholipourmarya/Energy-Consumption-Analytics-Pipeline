"""Publish CSV readings as keyed JSON messages on Kafka."""

import argparse
import csv
import json
from pathlib import Path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('csv', type=Path)
    parser.add_argument('--broker', default='127.0.0.1:9092')
    parser.add_argument('--topic', default='energy-readings')
    args = parser.parse_args()

    from kafka import KafkaProducer

    with args.csv.open(encoding='utf-8', newline='') as handle:
        rows = csv.DictReader(handle, strict=True)
        required = {'reading_id', 'meter_id', 'date', 'consumption_kwh'}
        if not required.issubset(rows.fieldnames or ()):
            parser.error(f'CSV header must include {", ".join(sorted(required))}')

        count = 0
        producer = KafkaProducer(bootstrap_servers=args.broker, acks='all',
                                 enable_idempotence=True, allow_auto_create_topics=False)
        try:
            for row in rows:
                if None in row:
                    raise ValueError(f'Extra CSV fields at line {rows.line_num}')
                reading_id = (row['reading_id'] or '').strip()
                key = reading_id.encode('utf-8') if reading_id else None
                value = json.dumps(row, allow_nan=False).encode('utf-8')
                producer.send(args.topic, key=key, value=value).get(timeout=30)
                count += 1
        finally:
            producer.close(timeout=5)
    print(json.dumps({'topic': args.topic, 'published': count}))


if __name__ == '__main__':
    main()
