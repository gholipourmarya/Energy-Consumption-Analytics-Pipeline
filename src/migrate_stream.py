"""Copy a legacy SQLite streaming warehouse to PostgreSQL before resuming Spark."""

import argparse
import json
import sqlite3
from contextlib import closing
from pathlib import Path
from uuid import UUID

from src.stream_store import connect, initialize, write_checkpoint_binding


def migrate(state_dir):
    if (state_dir / 'postgres-warehouse-id').exists():
        raise RuntimeError(f'State directory is already bound to PostgreSQL: {state_dir}')
    metadata = state_dir / 'checkpoint' / 'metadata'
    checkpoint_id = UUID(json.loads(metadata.read_text(encoding='utf-8'))['id'])
    warehouse = state_dir / 'warehouse.db'
    warehouse_id = initialize()

    with closing(sqlite3.connect(f'{warehouse.as_uri()}?mode=ro', uri=True)) as source, connect() as target:
        source.execute('BEGIN')
        target.execute('LOCK TABLE public.stream_events, public.quality_batches IN SHARE ROW EXCLUSIVE MODE')
        imported = target.execute('SELECT legacy_checkpoint_id FROM public.warehouse_identity').fetchone()[0]
        if imported != checkpoint_id:
            nonempty = target.execute(
                'SELECT EXISTS (SELECT 1 FROM public.stream_events) '
                'OR EXISTS (SELECT 1 FROM public.quality_batches)'
            ).fetchone()[0]
            if imported is not None or nonempty:
                raise RuntimeError('SQLite migration requires an empty PostgreSQL streaming warehouse')
            with target.cursor() as cursor:
                cursor.executemany(
                    'INSERT INTO public.stream_events VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)',
                    source.execute(
                        'SELECT topic, kafka_partition, kafka_offset, reading_id, meter_id, reading_date, '
                        'consumption_kwh, high_consumption, rejection_reason, payload FROM stream_events'
                    ),
                )
                cursor.executemany(
                    'INSERT INTO public.quality_batches '
                    '(spark_batch_id, recorded_at, accepted, rejected, duplicates) VALUES (%s, %s, %s, %s, %s)',
                    source.execute(
                        'SELECT spark_batch_id, recorded_at, accepted, rejected, duplicates '
                        'FROM quality_batches ORDER BY id'
                    ),
                )
            target.execute('UPDATE public.warehouse_identity SET legacy_checkpoint_id = %s', (checkpoint_id,))

    write_checkpoint_binding(state_dir, warehouse_id)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--state-dir', type=Path, default=Path('output/stream'))
    migrate(parser.parse_args().state_dir.resolve())


if __name__ == '__main__':
    main()
