#!/bin/bash
set -e

# This script runs after the 01_create_tables.sql script.

echo "Executing automated data copy for patients, encounters, blood pressures, and blood sugars..."

# --- Load Patients Data ---
# Note: Uses ON CONFLICT to handle duplicates (consistent with insert_heart360_data function)
if [ -f "/var/lib/postgresql/import/patients_data.csv" ]; then
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
        SET datestyle TO 'ISO, MDY';
        SET ROLE heart360tk;
        SET SEARCH_PATH = heart360tk_schema;

        -- Load patients using temp table to handle duplicates
        CREATE TEMP TABLE temp_patients (
            patient_id BIGINT,
            patient_status VARCHAR(10),
            registration_date TIMESTAMP,
            death_date DATE,
            facility VARCHAR(255),
            region VARCHAR(255)
        );

        \COPY temp_patients (patient_id, patient_status, registration_date, death_date, facility, region) FROM '/var/lib/postgresql/import/patients_data.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', NULL 'NaT');

        -- Lookup org_unit_id from org_units table by matching name
        INSERT INTO patients (patient_id, patient_status, registration_date, death_date, org_unit_id)
        SELECT tp.patient_id, tp.patient_status, tp.registration_date, tp.death_date, ou.id
        FROM temp_patients tp
        LEFT JOIN org_units ou ON ou.name = tp.facility
        ON CONFLICT (patient_id)
        DO UPDATE SET
            patient_status = EXCLUDED.patient_status,
            registration_date = LEAST(patients.registration_date, EXCLUDED.registration_date),
            death_date = COALESCE(EXCLUDED.death_date, patients.death_date),
            org_unit_id = COALESCE(EXCLUDED.org_unit_id, patients.org_unit_id);

        DROP TABLE temp_patients;
EOSQL
else
    echo "Warning: patients_data.csv not found, skipping patients load."
fi

# --- Load Encounters Data ---
# Note: Uses ON CONFLICT to handle duplicates (consistent with insert_heart360_data function)
if [ -f "/var/lib/postgresql/import/encounters_data.csv" ]; then
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
        SET datestyle TO 'ISO, MDY';
        SET ROLE heart360tk;
        SET SEARCH_PATH = heart360tk_schema;

        -- Load encounters using temp table to handle duplicates
        CREATE TEMP TABLE temp_encounters (
            patient_id BIGINT,
            encounter_date TIMESTAMP,
            facility VARCHAR(255),
            region VARCHAR(255)
        );

        \COPY temp_encounters (patient_id, encounter_date, facility, region) FROM '/var/lib/postgresql/import/encounters_data.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', NULL 'NaT');

        -- Lookup org_unit_id from org_units table by matching name
        INSERT INTO encounters (patient_id, encounter_date, org_unit_id)
        SELECT te.patient_id, te.encounter_date, ou.id
        FROM temp_encounters te
        LEFT JOIN org_units ou ON ou.name = te.facility
        ON CONFLICT (patient_id, encounter_date)
        DO UPDATE SET
            org_unit_id = EXCLUDED.org_unit_id;

        DROP TABLE temp_encounters;
EOSQL
else
    echo "Warning: encounters_data.csv not found. If you have bp_encounters_data.csv, please regenerate CSVs using prepare_date.py"
fi

# --- Load Blood Pressures Data ---
# Note: This requires matching encounters by (patient_id, encounter_date) to get encounter_id
# Handles multiple BP readings per encounter by taking the first one (consistent with codebase logic)
if [ -f "/var/lib/postgresql/import/blood_pressures_data.csv" ]; then
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
        SET datestyle TO 'ISO, MDY';
        SET ROLE heart360tk;
        SET SEARCH_PATH = heart360tk_schema;

        -- Create temporary table for blood pressures data
        CREATE TEMP TABLE temp_blood_pressures (
            patient_id BIGINT,
            encounter_date TIMESTAMP,
            systolic_bp NUMERIC,
            diastolic_bp NUMERIC
        );

        -- Load data into temp table
        \COPY temp_blood_pressures (patient_id, encounter_date, systolic_bp, diastolic_bp) FROM '/var/lib/postgresql/import/blood_pressures_data.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', NULL 'NaT');

        -- Handle duplicates: if multiple BP readings for same encounter, take the first one
        -- Insert into blood_pressures by matching encounters
        INSERT INTO blood_pressures (encounter_id, systolic_bp, diastolic_bp)
        SELECT DISTINCT ON (e.id)
            e.id AS encounter_id,
            tbp.systolic_bp,
            tbp.diastolic_bp
        FROM temp_blood_pressures tbp
        JOIN encounters e
            ON e.patient_id = tbp.patient_id
            AND e.encounter_date = tbp.encounter_date
        WHERE tbp.systolic_bp IS NOT NULL OR tbp.diastolic_bp IS NOT NULL
        ORDER BY e.id, tbp.systolic_bp NULLS LAST, tbp.diastolic_bp NULLS LAST;

        DROP TABLE temp_blood_pressures;
EOSQL
else
    echo "Warning: blood_pressures_data.csv not found, skipping blood pressures load."
fi

# --- Load Blood Sugars Data ---
# Note: This requires matching encounters by (patient_id, encounter_date) to get encounter_id
# Handles multiple BS readings per encounter by taking the first one (consistent with codebase logic)
if [ -f "/var/lib/postgresql/import/blood_sugars_data.csv" ]; then
    psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
        SET datestyle TO 'ISO, MDY';
        SET ROLE heart360tk;
        SET SEARCH_PATH = heart360tk_schema;

        -- Create temporary table for blood sugars data
        CREATE TEMP TABLE temp_blood_sugars (
            patient_id BIGINT,
            encounter_date TIMESTAMP,
            blood_sugar_type VARCHAR(50),
            blood_sugar_value NUMERIC
        );

        -- Load data into temp table
        \COPY temp_blood_sugars (patient_id, encounter_date, blood_sugar_type, blood_sugar_value) FROM '/var/lib/postgresql/import/blood_sugars_data.csv' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', NULL 'NaT');

        -- Handle duplicates: if multiple BS readings for same encounter, take the first one
        -- Insert into blood_sugars by matching encounters
        INSERT INTO blood_sugars (encounter_id, blood_sugar_type, blood_sugar_value)
        SELECT DISTINCT ON (e.id)
            e.id AS encounter_id,
            tbs.blood_sugar_type,
            tbs.blood_sugar_value
        FROM temp_blood_sugars tbs
        JOIN encounters e
            ON e.patient_id = tbs.patient_id
            AND e.encounter_date = tbs.encounter_date
        WHERE tbs.blood_sugar_value IS NOT NULL
        ORDER BY e.id, tbs.blood_sugar_value NULLS LAST;

        DROP TABLE temp_blood_sugars;
EOSQL
else
    echo "Warning: blood_sugars_data.csv not found, skipping blood sugars load."
fi

echo "Data copy completed successfully."
