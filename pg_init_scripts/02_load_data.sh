#!/bin/bash
set -e

# This script runs after the 01_create_tables.sql script.

echo "Executing automated data copy for patients and encounters..."

# --- Load Patients Data ---
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    SET datestyle TO 'ISO, MDY';
    \COPY heart360tk_schema.patients (patient_id,patient_status,registration_date,death_date, facility, region) FROM '/var/lib/postgresql/import/patients_data.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', NULL 'NaT');
EOSQL

# --- Load BP Encounters Data ---
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    \COPY heart360tk_schema.bp_encounters FROM '/var/lib/postgresql/import/bp_encounters_data.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', NULL 'NaT');
EOSQL

echo "Data copy completed successfully."
