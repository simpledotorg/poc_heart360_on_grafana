CREATE USER grafana WITH PASSWORD 'ZDI5MGE3YmYyMTRlNGNmYThkMTYyZjk3';
CREATE SCHEMA grafana_schema AUTHORIZATION grafana;
ALTER ROLE grafana SET search_path TO grafana_schema, public;


CREATE USER heart360tk WITH PASSWORD 'YmE4ZTk0OGI0OTNmNGU5YmFjZDY1YTA0';
CREATE SCHEMA heart360tk_schema AUTHORIZATION heart360tk;
ALTER ROLE heart360tk SET search_path TO heart360tk_schema, public;
