"""Validate and ingest Kafka readings with Spark Structured Streaming."""

import argparse
import logging
from pathlib import Path
from time import monotonic

from pyspark.sql import SparkSession, functions as F

from src.stream_store import bind_checkpoint, initialize, persist_batch

logger = logging.getLogger(__name__)

PAYLOAD_SCHEMA = '''
    reading_id STRING,
    meter_id STRING,
    date STRING,
    consumption_kwh STRING,
    _corrupt_record STRING
'''


def validate(messages):
    data = (
        messages.select('topic', 'partition', 'offset', F.col('value').cast('string').alias('payload'))
        .withColumn('event', F.from_json('payload', PAYLOAD_SCHEMA, {
            'columnNameOfCorruptRecord': '_corrupt_record',
            'allowSingleQuotes': 'false',
            'allowNonNumericNumbers': 'false',
        }))
        .withColumn('reading_id', F.regexp_replace('event.reading_id', r'^\s+|\s+$', ''))
        .withColumn('meter_id', F.regexp_replace('event.meter_id', r'^\s+|\s+$', ''))
        .withColumn('reading_date', F.col('event.date').try_cast('date'))
        .withColumn('consumption_kwh', F.col('event.consumption_kwh').try_cast('double'))
    )
    consumption = F.col('consumption_kwh')
    reason = (
        F.when(F.col('event').isNull() | F.col('event._corrupt_record').isNotNull(), 'invalid_json')
        .when(F.coalesce('reading_id', F.lit('')) == '', 'missing_reading_id')
        .when(F.coalesce('meter_id', F.lit('')) == '', 'missing_meter_id')
        .when(F.col('reading_id').contains('\x00') | F.col('meter_id').contains('\x00'), 'invalid_identifier')
        .when(
            F.col('reading_date').isNull() | ~F.col('event.date').rlike(r'^\d{4}-\d{2}-\d{2}$')
            | F.col('event.date').startswith('0000-'),
            'invalid_date',
        )
        .when(consumption.isNull() | F.isnan(consumption) | (F.abs(consumption) == float('inf')), 'invalid_consumption')
        .when(consumption < 0, 'negative_consumption')
    )
    return data.select(
        'topic', 'partition', 'offset',
        F.regexp_replace('payload', r'\x00', r'\\u0000').alias('payload'),
        F.when(~F.col('reading_id').contains('\x00'), F.col('reading_id')).alias('reading_id'),
        F.when(~F.col('meter_id').contains('\x00'), F.col('meter_id')).alias('meter_id'),
        F.when(F.year('reading_date').between(1, 9999), F.col('reading_date')).alias('reading_date'),
        'consumption_kwh', (consumption > 30).cast('int').alias('high_consumption'),
        reason.alias('reason'),
    )


def start_stream(spark, broker, topic, state_dir, max_offsets, follow=False):
    checkpoint = bind_checkpoint(state_dir, initialize())

    messages = (
        spark.readStream.format('kafka')
        .option('kafka.bootstrap.servers', broker)
        .option('subscribe', topic)
        .option('startingOffsets', 'earliest')
        .option('maxOffsetsPerTrigger', max_offsets)
        .load()
    )

    def write_batch(batch, batch_id):
        started = monotonic()
        try:
            rows = batch.orderBy('partition', 'offset').toLocalIterator()
            accepted, rejected, duplicates, replayed = persist_batch(rows, batch_id)
        except Exception:
            logger.exception('stream_batch_failed topic=%s batch_id=%s', topic, batch_id)
            raise
        logger.info(
            'stream_batch_completed topic=%s batch_id=%s accepted=%s rejected=%s '
            'duplicates=%s replayed=%s duration_seconds=%.3f',
            topic, batch_id, accepted, rejected, duplicates, replayed, monotonic() - started,
        )

    writer = (
        validate(messages).writeStream
        .option('checkpointLocation', str(checkpoint))
        .foreachBatch(write_batch)
    )
    if follow:
        return writer.trigger(processingTime='5 seconds').start()
    return writer.trigger(availableNow=True).start()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--broker', default='127.0.0.1:9092')
    parser.add_argument('--topic', default='energy-readings')
    parser.add_argument('--state-dir', type=Path, default=Path('output/stream'))
    parser.add_argument('--max-offsets', type=int, default=10_000)
    parser.add_argument('--follow', action='store_true')
    args = parser.parse_args()
    if args.max_offsets < 1:
        parser.error('--max-offsets must be positive')

    from pyspark import __version__

    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(name)s %(message)s')
    logger.info('stream_starting topic=%s state_dir=%s follow=%s', args.topic, args.state_dir, args.follow)
    spark = (
        SparkSession.builder.appName('Energy Consumption Streaming')
        .master('local[2]')
        .config('spark.jars.packages', f'org.apache.spark:spark-sql-kafka-0-10_2.13:{__version__}')
        .config('spark.sql.shuffle.partitions', '2')
        .config('spark.sql.session.timeZone', 'UTC')
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel('WARN')
    try:
        query = start_stream(
            spark, args.broker, args.topic, args.state_dir.resolve(), args.max_offsets, args.follow
        )
        try:
            query.awaitTermination()
            logger.info('stream_completed topic=%s query_id=%s', args.topic, query.id)
        finally:
            if query.isActive:
                query.stop()
    finally:
        spark.stop()


if __name__ == '__main__':
    main()
