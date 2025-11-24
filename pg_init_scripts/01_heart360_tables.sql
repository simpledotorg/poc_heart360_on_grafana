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

