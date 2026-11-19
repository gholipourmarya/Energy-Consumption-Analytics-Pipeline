"""Build energy consumption marts and check recent streaming data quality."""

from datetime import timedelta

import pendulum
from airflow.providers.standard.operators.bash import BashOperator
from airflow.sdk import DAG

PROJECT = '/opt/energy'
PYTHON = f'{PROJECT}/.venv/bin/python'
DBT = f'{PROJECT}/.venv/bin/dbt'

with DAG(
    dag_id='energy_analytics',
    schedule='0 * * * *',
    start_date=pendulum.datetime(2026, 1, 1, tz='UTC'),
    catchup=False,
    max_active_runs=1,
    default_args={'retries': 1, 'retry_delay': timedelta(minutes=2)},
    tags=['energy'],
) as dag:
    initialize_warehouse = BashOperator(
        task_id='initialize_warehouse',
        cwd=PROJECT,
        bash_command=f'{PYTHON} -c "from src.stream_store import initialize; initialize()"',
    )

    build_reports = BashOperator(
        task_id='build_reports',
        cwd=PROJECT,
        bash_command=f'{DBT} build --project-dir dbt --profiles-dir dbt',
    )

    check_quality = BashOperator(
        task_id='check_quality',
        cwd=PROJECT,
        bash_command=(
            f'{PYTHON} -m src.quality --lookback-hours 24'
            ' --max-reject-rate 0.10 --max-duplicate-rate 0.05'
        ),
        skip_on_exit_code=2,
    )

    initialize_warehouse >> [build_reports, check_quality]
