FROM apache/airflow:3.2.2-python3.12

USER root
RUN install -d -o airflow -g root /opt/energy
USER airflow
WORKDIR /opt/energy
COPY --chown=airflow:root requirements-analytics.txt .
RUN python -m venv /opt/energy/.venv \
    && /opt/energy/.venv/bin/python -m pip install --no-cache-dir -r requirements-analytics.txt

COPY --chown=airflow:root src ./src
COPY --chown=airflow:root dbt/dbt_project.yml dbt/profiles.yml ./dbt/
COPY --chown=airflow:root dbt/models ./dbt/models
COPY --chown=airflow:root dbt/tests ./dbt/tests
COPY --chown=airflow:root airflow/dags /opt/airflow/dags
