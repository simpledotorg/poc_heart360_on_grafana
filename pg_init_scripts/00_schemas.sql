CREATE USER grafana WITH PASSWORD 'your_secure_password';
CREATE SCHEMA grafana_schema AUTHORIZATION grafana;
ALTER ROLE grafana SET search_path TO grafana_schema, public;