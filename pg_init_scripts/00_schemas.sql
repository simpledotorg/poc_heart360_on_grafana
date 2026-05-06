CREATE USER grafana WITH PASSWORD 'ZDI5MGE3YmYyMTRlNGNmYThkMTYyZjk3';
CREATE SCHEMA grafana_schema AUTHORIZATION grafana;
ALTER ROLE grafana SET search_path TO grafana_schema, public;


CREATE USER heart360tk WITH PASSWORD 'YmE4ZTk0OGI0OTNmNGU5YmFjZDY1YTA0';
CREATE SCHEMA heart360tk_schema AUTHORIZATION heart360tk;
CREATE SCHEMA heart360tk_reporting AUTHORIZATION heart360tk;
ALTER ROLE heart360tk SET search_path TO heart360tk_schema, heart360tk_reporting, public;
-- Grant permission to read files for COPY command (needed for data loading)
GRANT pg_read_server_files TO heart360tk;

CREATE EXTENSION IF NOT EXISTS pg_cron;

GRANT USAGE ON SCHEMA cron TO heart360tk;
GRANT EXECUTE ON ALL FUNCTIONS IN SCHEMA cron TO heart360tk;