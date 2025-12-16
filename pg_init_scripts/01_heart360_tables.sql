-- 1. Patients Table
CREATE TABLE patients (
    patient_id          bigint PRIMARY KEY,
    patient_name        VARCHAR(255),
    patient_status      VARCHAR(10) NOT NULL CHECK (patient_status IN ('DEAD', 'ALIVE')),
    registration_date   TIMESTAMP NOT NULL,
    birth_date          Date,
    death_date          DATE,           -- Nullable
    facility            VARCHAR(255),
    region              VARCHAR(255)
);

-- 2. BP Encounters Table
CREATE TABLE bp_encounters (
    encounter_id        bigint PRIMARY KEY,
    patient_id          bigint NOT NULL REFERENCES patients(patient_id), -- Foreign Key
    encounter_date      TIMESTAMP NOT NULL,
    diastolic_bp        NUMERIC,
    systolic_bp         NUMERIC
);



--drop view HEART360_PATIENTS_REGISTERED;
--drop view HEART360_PATIENTS_UNDER_CARE;
--drop view HEART360_PATIENTS_CATAGORY;

--
-- HEART360_PATIENTS_REGISTERED
--
CREATE OR REPLACE VIEW HEART360_PATIENTS_REGISTERED as
WITH
KNOWN_MONTHS AS (
  SELECT
    date_trunc('month', series_date)::date AS REF_MONTH
  FROM
    generate_series(
        (SELECT min(REGISTRATION_DATE) from patients),
        date_trunc('month', current_date),
        '1 month'::interval
    ) AS t(series_date)
),
PATIENTS_BY_MONTH AS (
    SELECT
        DATE_TRUNC('month',REGISTRATION_DATE) AS REF_MONTH,
        facility,
        count(*) AS NB_NEW_PATIENTS
    FROM patients
    WHERE PATIENT_STATUS <> 'dead'
    GROUP BY DATE_TRUNC('month',REGISTRATION_DATE), facility
)
SELECT
    KNOWN_MONTHS.REF_MONTH,
    REFERENCE_BEFORE.facility,
    sum(REFERENCE_BEFORE.NB_NEW_PATIENTS) AS CUMULATIVE_NUMBER_OF_PATIENTS,
    sum(case when KNOWN_MONTHS.REF_MONTH = REFERENCE_BEFORE.REF_MONTH then NB_NEW_PATIENTS else null end) AS NB_NEW_PATIENTS
FROM KNOWN_MONTHS
LEFT OUTER JOIN PATIENTS_BY_MONTH REFERENCE_BEFORE ON KNOWN_MONTHS.REF_MONTH >= REFERENCE_BEFORE.REF_MONTH
GROUP BY 1, 2
ORDER BY KNOWN_MONTHS.REF_MONTH DESC;

--
-- HEART360_PATIENTS_UNDER_CARE
--
CREATE OR REPLACE VIEW HEART360_PATIENTS_UNDER_CARE as
WITH
KNOWN_MONTHS AS (
  SELECT
    date_trunc('month', series_date)::date AS REF_MONTH
  FROM
    generate_series(
        (SELECT min(REGISTRATION_DATE) from patients),
        date_trunc('month', current_date),
        '1 month'::interval
    ) AS t(series_date)
),
ALIVE_PATIENTS AS (
    SELECT
        DATE_TRUNC('month',REGISTRATION_DATE) AS REGISTRATION_MONTH,
        facility,
        patient_id
    FROM patients
    WHERE PATIENT_STATUS <> 'dead'
),
BP_ENCOUNTERS AS (
    SELECT
        patient_id,
        DATE_TRUNC('month',ENCOUNTER_DATE) AS BP_ENCOUNTER_MONTH
    FROM BP_ENCOUNTERS
)
SELECT
    KNOWN_MONTHS.REF_MONTH,
    ALIVE_PATIENTS.facility,
    sum(CASE WHEN BP_ENCOUNTERS.patient_id IS NULL THEN 1 ELSE NULL END ) AS NB_PATIENTS_LOST_TO_FOLLOW_UP,
    count(DISTINCT(BP_ENCOUNTERS.patient_id)) AS NB_PATIENTS_UNDER_CARE,
    count(DISTINCT(ALIVE_PATIENTS.patient_id)) AS CUMULATIVE_NUMBER_OF_PATIENTS
FROM KNOWN_MONTHS
LEFT OUTER JOIN ALIVE_PATIENTS
    ON ALIVE_PATIENTS.REGISTRATION_MONTH <= KNOWN_MONTHS.REF_MONTH
LEFT OUTER JOIN BP_ENCOUNTERS
    ON BP_ENCOUNTERS.patient_id = ALIVE_PATIENTS.patient_id
        AND BP_ENCOUNTER_MONTH <= KNOWN_MONTHS.REF_MONTH
        AND BP_ENCOUNTER_MONTH + interval '12 month'> KNOWN_MONTHS.REF_MONTH
GROUP BY 1, 2
ORDER BY KNOWN_MONTHS.REF_MONTH DESC;



--
-- HEART360_PATIENTS_CATAGORY
--
CREATE OR REPLACE VIEW HEART360_PATIENTS_CATAGORY as
WITH
KNOWN_MONTHS AS (
  SELECT
    date_trunc('month', series_date)::date AS REF_MONTH
  FROM
    generate_series(
        (SELECT min(REGISTRATION_DATE) from patients), -- Replace '2023-08-15' with your starting date
        date_trunc('month', current_date),       -- The current date/time is converted to the start of the current month
        '1 month'::interval
    ) AS t(series_date)
),
ALIVE_PATIENTS AS (
    SELECT
        DATE_TRUNC('month',REGISTRATION_DATE) AS REGISTRATION_MONTH,
        facility,
        patient_id AS patient_id
    FROM patients
    WHERE PATIENT_STATUS <> 'dead'
),
BP_ENCOUNTERS AS (
    SELECT
        encounter_id as id,
        patient_id,
        systolic_bp as systolic,
        diastolic_bp as diastolic,
        encounter_date  AS BP_ENCOUNTER_DATE,
        DATE_TRUNC('month',encounter_date)  AS BP_ENCOUNTER_MONTH
    FROM bp_encounters
),
LATEST_BP_BY_MONTH_AND_PATIENT AS (
    WITH MOST_RECENT_BP_ENCOUNTER AS (
        SELECT
            KNOWN_MONTHS.REF_MONTH,
            MOST_RECENT_BP_ENCOUNTER.patient_id,
            MAX(MOST_RECENT_BP_ENCOUNTER.BP_ENCOUNTER_DATE) AS MOST_RECENT_BP_DATE
        FROM bp_encounters MOST_RECENT_BP_ENCOUNTER
        JOIN KNOWN_MONTHS
            ON DATE_TRUNC('month', MOST_RECENT_BP_ENCOUNTER.BP_ENCOUNTER_DATE) <= KNOWN_MONTHS.REF_MONTH
        GROUP BY KNOWN_MONTHS.REF_MONTH, patient_id)
    SELECT
        REF_MONTH, MOST_RECENT_BP_ENCOUNTER.patient_id,
        MAX(systolic) AS systolic,
        MAX(diastolic) AS diastolic,
        MAX(BP_ENCOUNTER_MONTH) AS BP_ENCOUNTER_MONTH
    FROM MOST_RECENT_BP_ENCOUNTER
    JOIN BP_ENCOUNTERS
        ON MOST_RECENT_BP_ENCOUNTER.MOST_RECENT_BP_DATE = BP_ENCOUNTERS.BP_ENCOUNTER_DATE
        AND MOST_RECENT_BP_ENCOUNTER.patient_id = BP_ENCOUNTERS.patient_id
    GROUP BY REF_MONTH, MOST_RECENT_BP_ENCOUNTER.patient_id
)
SELECT
    KNOWN_MONTHS.REF_MONTH,
    ALIVE_PATIENTS.facility,
    SUM(CASE WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH + interval '12 month' < KNOWN_MONTHS.REF_MONTH then 0 else 1 end) as NB_PATIENTS_UNDER_CARE,
    SUM(CASE WHEN ALIVE_PATIENTS.REGISTRATION_MONTH + interval '3 month' > KNOWN_MONTHS.REF_MONTH then 1 else 0 end ) AS NB_PATIENTS_NEWLY_REGISTERED,
    SUM(CASE
        WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH + interval '12 month' <  KNOWN_MONTHS.REF_MONTH then 0
        WHEN ALIVE_PATIENTS.REGISTRATION_MONTH + interval '3 month' > KNOWN_MONTHS.REF_MONTH then 0 else 1 end 
        ) AS NB_PATIENTS_UNDER_CARE_REGISTERED_BEFORE_THE_PAST_3_MONTHS,
    SUM(CASE WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH + interval '12 month' < KNOWN_MONTHS.REF_MONTH then 1 else 0 end) as NB_PATIENTS_LOST_TO_FOLLOW_UP,
    SUM(CASE
        WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH + interval '12 month' <  KNOWN_MONTHS.REF_MONTH then 0
        WHEN ALIVE_PATIENTS.REGISTRATION_MONTH + interval '3 month' > KNOWN_MONTHS.REF_MONTH then 1
        ELSE 0 END ) as NB_PATIENTS_NO_VISIT,
    SUM(CASE
        WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH + interval '12 month' <  KNOWN_MONTHS.REF_MONTH then 0
        WHEN ALIVE_PATIENTS.REGISTRATION_MONTH + interval '3 month' > KNOWN_MONTHS.REF_MONTH then 0
        WHEN systolic > 140 OR diastolic > 90 then 1
        ELSE 0 END ) AS NB_PATIENTS_UNCONTROLLED,
    SUM(CASE
        WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH + interval '12 month' <  KNOWN_MONTHS.REF_MONTH then 0
        WHEN ALIVE_PATIENTS.REGISTRATION_MONTH + interval '3 month' > KNOWN_MONTHS.REF_MONTH then 0
        WHEN systolic > 140 OR diastolic > 90 then 0
        ELSE 1 END ) AS NB_PATIENTS_CONTROLLED
FROM KNOWN_MONTHS
LEFT OUTER JOIN ALIVE_PATIENTS
    ON ALIVE_PATIENTS.REGISTRATION_MONTH <= KNOWN_MONTHS.REF_MONTH
LEFT OUTER JOIN LATEST_BP_BY_MONTH_AND_PATIENT
    ON LATEST_BP_BY_MONTH_AND_PATIENT.patient_id = ALIVE_PATIENTS.patient_id
    AND LATEST_BP_BY_MONTH_AND_PATIENT.REF_MONTH = KNOWN_MONTHS.REF_MONTH
GROUP BY 1, 2
ORDER BY 1 DESC;


--
-- POC SPECIFIC INSERT STATEMENT
--
CREATE SEQUENCE bp_encounters_encounter_id_seq START WITH 6000000;

CREATE OR REPLACE FUNCTION insert_heart360_data(
    p_patient_id        bigint,
    p_patient_name      VARCHAR,
    p_birth_date        DATE,
    p_facility          VARCHAR,
    p_region            VARCHAR,
    p_encounter_datetime TIMESTAMP,
    p_diastolic_bp      NUMERIC,
    p_systolic_bp       NUMERIC
)
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    -- 1. Patients Table Logic
    IF EXISTS (SELECT 1 FROM patients WHERE patient_id = p_patient_id) THEN
        -- Patient exists: Update registration_date if the new encounter date is earlier (more recent)
        UPDATE patients
        SET
            registration_date = p_encounter_datetime
        WHERE
            patient_id = p_patient_id
            AND registration_date > p_encounter_datetime;

    ELSE
        -- Patient does not exist: Insert the new patient (Concise format)
        INSERT INTO patients (patient_id, patient_name, patient_status, registration_date, birth_date, facility, region)
        VALUES (p_patient_id, p_patient_name, 'ALIVE', p_encounter_datetime::DATE, p_birth_date, p_facility, p_region);
    END IF;

    ---
    -- 2. BP Encounters Table Logic
    ---

    -- Check if the specific (patient_id, encounter_date) pair already exists
    IF EXISTS (
        SELECT 1
        FROM bp_encounters
        WHERE
            patient_id = p_patient_id
            AND encounter_date = p_encounter_datetime
    ) THEN
        -- If it exists, do nothing (as requested)
        RAISE NOTICE 'BP encounter for patient % on % already exists. Skipping insertion.', p_patient_id, p_encounter_datetime;
    ELSE
        -- If it does not exist, insert the new encounter (Concise format)
        INSERT INTO bp_encounters (patient_id, encounter_id, encounter_date, diastolic_bp, systolic_bp)
        VALUES (p_patient_id, nextval('bp_encounters_encounter_id_seq'), p_encounter_datetime, p_diastolic_bp, p_systolic_bp);
    END IF;

END;
$$;

