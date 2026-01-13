CREATE USER grafana WITH PASSWORD 'your_secure_password';
CREATE SCHEMA grafana_schema AUTHORIZATION grafana;
ALTER ROLE grafana SET search_path TO grafana_schema, public;


CREATE USER heart360tk WITH PASSWORD 'your_db_password';
CREATE SCHEMA heart360tk_schema AUTHORIZATION heart360tk;
ALTER ROLE heart360tk SET search_path TO heart360tk_schema, public;