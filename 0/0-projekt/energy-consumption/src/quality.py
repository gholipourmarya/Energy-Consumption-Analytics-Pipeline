"""Report recent PostgreSQL streaming metrics and check quality thresholds."""

import argparse
import json

from psycopg.rows import dict_row

from src.stream_store import connect


def report(batches=None, lookback_hours=None):
    with connect() as con, con.cursor(row_factory=dict_row) as cursor:
        if lookback_hours is None:
            cursor.execute('SELECT * FROM public.quality_batches ORDER BY id DESC LIMIT %s', (batches if batches is not None else 1,))
        else:
            cursor.execute(
                'SELECT * FROM public.quality_batches '
                "WHERE recorded_at >= CURRENT_TIMESTAMP - %s * INTERVAL '1 hour' "
                'ORDER BY id DESC LIMIT %s',
                (lookback_hours, batches),
            )
        rows = cursor.fetchall()
    for row in rows:
        row['recorded_at'] = row['recorded_at'].isoformat()
    accepted = sum(row['accepted'] for row in rows)
    rejected = sum(row['rejected'] for row in rows)
    duplicates = sum(row['duplicates'] for row in rows)
    total = accepted + rejected + duplicates
    return {
        'batches': list(reversed(rows)),
        'accepted': accepted,
        'rejected': rejected,
        'duplicates': duplicates,
        'total': total,
        'reject_rate': rejected / total if total else None,
        'duplicate_rate': duplicates / total if total else None,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--batches', type=int)
    parser.add_argument('--lookback-hours', type=float)
    parser.add_argument('--max-reject-rate', type=float, default=0.10)
    parser.add_argument('--max-duplicate-rate', type=float, default=1.0)
    args = parser.parse_args()
    if args.batches is not None and args.batches < 1:
        parser.error('--batches must be positive')
    if args.lookback_hours is not None and not (0 < args.lookback_hours < float('inf')):
        parser.error('--lookback-hours must be finite and positive')
    if not (0 <= args.max_reject_rate <= 1 and 0 <= args.max_duplicate_rate <= 1):
        parser.error('Rate thresholds must be between 0 and 1')

    metrics = report(args.batches, args.lookback_hours)
    if metrics['total'] == 0:
        metrics['status'] = 'no_data'
        print(json.dumps(metrics, indent=2))
        return 2
    alerts = []
    if metrics['reject_rate'] > args.max_reject_rate:
        alerts.append('reject_rate_exceeded')
    if metrics['duplicate_rate'] > args.max_duplicate_rate:
        alerts.append('duplicate_rate_exceeded')
    metrics['status'] = 'alert' if alerts else 'ok'
    metrics['alerts'] = alerts
    print(json.dumps(metrics, indent=2))
    return int(bool(alerts))


if __name__ == '__main__':
    raise SystemExit(main())
