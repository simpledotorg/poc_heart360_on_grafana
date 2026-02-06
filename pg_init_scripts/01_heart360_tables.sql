SET ROLE heart360tk;
SET SEARCH_PATH = heart360tk_schema;

-- 1. Patients Table
CREATE TABLE IF NOT EXISTS patients (
    patient_id          bigint PRIMARY KEY,
    patient_name        VARCHAR(255),
    gender              VARCHAR(255),
    phone_number        VARCHAR(255),
    patient_status      VARCHAR(10) NOT NULL CHECK (patient_status IN ('DEAD', 'ALIVE')),
    registration_date   TIMESTAMP NOT NULL,
    birth_date          Date,
    death_date          DATE,           -- Nullable
    facility            VARCHAR(255),
    region              VARCHAR(255)
);

-- 2. BP Encounters Table
CREATE TABLE IF NOT EXISTS bp_encounters (
    encounter_id        bigint PRIMARY KEY,
    patient_id          bigint NOT NULL REFERENCES patients(patient_id),
    encounter_date      TIMESTAMP NOT NULL,
    diastolic_bp        NUMERIC,
    systolic_bp         NUMERIC
);

-- 3. Patient Calls
CREATE TABLE IF NOT EXISTS reminder_calls (
    patient_id          bigint NOT NULL REFERENCES patients(patient_id),
    call_date           TIMESTAMP NOT NULL,
    Call_result         VARCHAR(255)
);




drop view IF EXISTS HEART360_PATIENTS_REGISTERED;
drop view IF EXISTS HEART360_PATIENTS_UNDER_CARE;
drop view IF EXISTS HEART360_PATIENTS_CATAGORY;
drop view IF EXISTS HEART360_OVERDUE_PATIENTS;
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
    count(*) as TOTAL_NUMBER_OF_PATIENTS,
    SUM(CASE WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH + interval '12 month' < KNOWN_MONTHS.REF_MONTH then 0 else 1 end) as NB_PATIENTS_UNDER_CARE,
    SUM(CASE WHEN ALIVE_PATIENTS.REGISTRATION_MONTH + interval '3 month' > KNOWN_MONTHS.REF_MONTH then 1 else 0 end ) AS NB_PATIENTS_NEWLY_REGISTERED,
    SUM(CASE
        WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH + interval '12 month' <  KNOWN_MONTHS.REF_MONTH then 0
        WHEN ALIVE_PATIENTS.REGISTRATION_MONTH + interval '3 month' > KNOWN_MONTHS.REF_MONTH then 0 else 1 end 
        ) AS NB_PATIENTS_UNDER_CARE_REGISTERED_BEFORE_THE_PAST_3_MONTHS,
    SUM(CASE WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH + interval '12 month' < KNOWN_MONTHS.REF_MONTH then 1 else 0 end) as NB_PATIENTS_LOST_TO_FOLLOW_UP,
    SUM(CASE
        WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH + interval '12 month' <  KNOWN_MONTHS.REF_MONTH then 0
        WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH + interval '3 month' <  KNOWN_MONTHS.REF_MONTH then 1
        ELSE 0 END ) as NB_PATIENTS_NO_VISIT,
    SUM(CASE
        WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH + interval '3 month' <  KNOWN_MONTHS.REF_MONTH then 0
        WHEN ALIVE_PATIENTS.REGISTRATION_MONTH + interval '3 month' > KNOWN_MONTHS.REF_MONTH then 0
        WHEN systolic > 140 OR diastolic > 90 then 1
        ELSE 0 END ) AS NB_PATIENTS_UNCONTROLLED,
    SUM(CASE
        WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH + interval '3 month' <  KNOWN_MONTHS.REF_MONTH then 0
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
-- View for overdue patients
--
CREATE OR REPLACE VIEW HEART360_OVERDUE_PATIENTS as
WITH MOST_RECENT_ENCOUNTER as (
    select * from bp_encounters where (patient_id, encounter_date) in (
    select 
        patient_id, 
        max(encounter_date) as last_encounter_date
from bp_encounters
    group by patient_id)),
MOST_RECENT_CALL as (
    select * from reminder_calls where (patient_id, call_date) in (select 
        patient_id, 
        max(call_date) as last_call_date
from reminder_calls
    group by patient_id))
select 
    patients.patient_id,
    patients.patient_name, 
    patients.registration_date,
    patients.birth_date,
    patients.gender,
    patients.phone_number,
    patients.facility,
    patients.region,
    MOST_RECENT_ENCOUNTER.encounter_date as last_visit_date,
    MOST_RECENT_ENCOUNTER.diastolic_bp as last_bp_diastolic,
    MOST_RECENT_ENCOUNTER.systolic_bp as last_bp_systolic,
    call_date as last_call_date,
    call_result as last_call_result
from patients
left outer join MOST_RECENT_ENCOUNTER on patients.patient_id= MOST_RECENT_ENCOUNTER.patient_id
left outer join MOST_RECENT_CALL on patients.patient_id= MOST_RECENT_CALL.patient_id and call_date > encounter_date
;


--
-- View for Patient Cohort Classification
--
CREATE OR REPLACE VIEW HEART360_COHORT_PATIENT_DETAILS as
WITH patients_quarter as (SELECT
    patient_id, facility,
    date_trunc('quarter', registration_date) as registration_quarter,
    date_trunc('quarter', registration_date) + interval '6 month' as cohort_validation_month,
 registration_date
FROM patients),
LAST_BP_IN_INTERVAL as (
    select *
    from bp_encounters 
    where encounter_id in (
        select max(encounter_id) as encounter_id
        from bp_encounters
        where (patient_id, encounter_date) in (
            SELECT 
                patients_quarter.patient_id, 
                max(encounter_date) as most_recent_bp 
            from bp_encounters
            join patients_quarter 
                on patients_quarter.patient_id=bp_encounters.patient_id
                and bp_encounters.encounter_date < patients_quarter.cohort_validation_month
            group by patients_quarter.patient_id)
        group by patient_id))
select 
    patients_quarter.patient_id, 
    facility, 
    registration_quarter,
    case 
        when encounter_date IS NULL then 'missed visit'
        when cohort_validation_month > encounter_date + interval '3 month'  then 'missed visit' 
        when diastolic_bp <  90 and  systolic_bp < 140 then 'controlled'
        else 'uncontrolled' end as status_at_end_of_interval
from patients_quarter 
left outer join LAST_BP_IN_INTERVAL on LAST_BP_IN_INTERVAL.patient_id = patients_quarter.patient_id
;




--
-- POC SPECIFIC INSERT STATEMENT
--
CREATE SEQUENCE IF NOT EXISTS bp_encounters_encounter_id_seq START WITH 6000000;

CREATE OR REPLACE FUNCTION insert_heart360_data(
    p_patient_id        bigint,
    p_patient_name      VARCHAR,
    p_gender            VARCHAR,
    p_phone_number      VARCHAR,
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
        INSERT INTO patients (patient_id, patient_name, gender, phone_number, patient_status, registration_date, birth_date, facility, region)
        VALUES (p_patient_id, p_patient_name, p_gender, p_phone_number, 'ALIVE', p_encounter_datetime::DATE, p_birth_date, p_facility, p_region);
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


