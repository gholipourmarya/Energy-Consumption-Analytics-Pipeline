#!/usr/bin/env bash
set -euo pipefail

psql --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" --set=ON_ERROR_STOP=1 \
  --set=owner="$POSTGRES_USER" --set=grafana_password="$ENERGY_GRAFANA_DB_PASSWORD" <<'SQL'
CREATE ROLE energy_grafana LOGIN PASSWORD :'grafana_password';
CREATE SCHEMA IF NOT EXISTS analytics AUTHORIZATION :"owner";
GRANT USAGE ON SCHEMA analytics TO energy_grafana;
ALTER DEFAULT PRIVILEGES FOR ROLE :"owner" IN SCHEMA analytics
    GRANT SELECT ON TABLES TO energy_grafana;
SQL
