-- 1. Patients Table
CREATE TABLE patients (
    patient_id          INT PRIMARY KEY,
    patient_status      VARCHAR(10) NOT NULL CHECK (patient_status IN ('DEAD', 'ALIVE')),
    registration_date   DATE NOT NULL,
    death_date          DATE,           -- Nullable
    facility            VARCHAR(255),
    region              VARCHAR(255)
);

-- 2. BP Encounters Table
CREATE TABLE bp_encounters (
    encounter_id        INT PRIMARY KEY,
    patient_id          INT NOT NULL REFERENCES patients(patient_id), -- Foreign Key
    encounter_date      DATE NOT NULL,
    diastolic_bp        NUMERIC,
    systolic_bp         NUMERIC
);


CREATE OR REPLACE VIEW HEART360_REGISTERED_PATIENTS as
WITH
KNOWN_MONTHS AS (
    SELECT
        DISTINCT(DATE_TRUNC('month',REGISTRATION_DATE)) AS REF_MONTH
    FROM public.patients
),
PATIENTS_BY_MONTH AS (
    SELECT
        DATE_TRUNC('month',REGISTRATION_DATE) AS REF_MONTH,
        count(*) AS NB_NEW_PATIENTS
    FROM public.patients
    WHERE PATIENT_STATUS <> 'dead'
    GROUP BY DATE_TRUNC('month',REGISTRATION_DATE)
)
SELECT
    KNOWN_MONTHS.REF_MONTH,
    sum(REFERENCE_BEFORE.NB_NEW_PATIENTS) AS CUMULATIVE_NUMBER_OF_PATIENTS,
    sum(case when KNOWN_MONTHS.REF_MONTH = REFERENCE_BEFORE.REF_MONTH then NB_NEW_PATIENTS else null end) AS NB_NEW_PATIENTS
FROM KNOWN_MONTHS
JOIN PATIENTS_BY_MONTH REFERENCE_BEFORE ON KNOWN_MONTHS.REF_MONTH >= REFERENCE_BEFORE.REF_MONTH
GROUP BY KNOWN_MONTHS.REF_MONTH
ORDER BY KNOWN_MONTHS.REF_MONTH DESC;

