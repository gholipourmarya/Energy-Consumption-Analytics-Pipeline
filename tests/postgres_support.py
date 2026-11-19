"""Isolate PostgreSQL integration tests in temporary databases."""

import os
from unittest.mock import patch
from uuid import uuid4

POSTGRES_TEST_DB = os.environ.get('ENERGY_TEST_PG_DATABASE')


def use_test_database(test):
    from psycopg import sql
    from src.stream_store import connect

    control = patch.dict(os.environ, ENERGY_PG_DATABASE=POSTGRES_TEST_DB)
    control.start()
    test.addCleanup(control.stop)
    database_name = f'energy_test_{uuid4().hex}'
    database = sql.Identifier(database_name)
    admin = connect()
    admin.autocommit = True
    test.addCleanup(admin.close)
    admin.execute(sql.SQL('CREATE DATABASE {}').format(database))
    test.addCleanup(admin.execute, sql.SQL('DROP DATABASE {} WITH (FORCE)').format(database))
    runtime = patch.dict(os.environ, ENERGY_PG_DATABASE=database_name)
    runtime.start()
    test.addCleanup(runtime.stop)
