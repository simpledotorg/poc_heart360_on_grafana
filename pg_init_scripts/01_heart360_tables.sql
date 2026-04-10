SET ROLE heart360tk;
SET search_path TO heart360tk_schema;
-- ============================================================================
-- ORG_UNITS: Dynamic hierarchy table (replaces fixed facilities table)
-- Level 0 = root/country, 1 = region, 2 = district, etc.
-- Hierarchy depth is determined purely by ingestion data, not schema.
-- ============================================================================
CREATE TABLE IF NOT EXISTS org_units (
    id          SERIAL PRIMARY KEY,
    name        VARCHAR(255) NOT NULL,
    level       INTEGER NOT NULL,
    parent_id   INTEGER REFERENCES org_units(id)
);

-- Unique constraints: handle NULL parent_id (root nodes) separately
CREATE UNIQUE INDEX IF NOT EXISTS org_units_unique_root
    ON org_units(name, level) WHERE parent_id IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS org_units_unique_child
    ON org_units(name, level, parent_id) WHERE parent_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_org_units_parent_id ON org_units(parent_id);
CREATE INDEX IF NOT EXISTS idx_org_units_level ON org_units(level);
CREATE INDEX IF NOT EXISTS idx_org_units_name ON org_units(name);

-- ============================================================================
-- CORE DATA TABLES
-- ============================================================================

-- 1. Patients Table
CREATE TABLE IF NOT EXISTS patients (
    patient_id          bigint PRIMARY KEY,
    patient_name        VARCHAR(255),
    gender              VARCHAR(255),
    phone_number        VARCHAR(255),
    patient_status      VARCHAR(10) NOT NULL CHECK (patient_status IN ('DEAD', 'ALIVE')),
    registration_date   TIMESTAMP NOT NULL,
    birth_date          Date,
    death_date          DATE,
    org_unit_id         INTEGER REFERENCES org_units(id)
);

CREATE INDEX IF NOT EXISTS idx_patients_org_unit_id ON patients(org_unit_id);

-- 2. Drop old BP Encounters Table (if exists)
DROP TABLE IF EXISTS bp_encounters CASCADE;

-- 3. Encounters Table
CREATE TABLE encounters (
    id              BIGSERIAL PRIMARY KEY,
    patient_id      BIGINT NOT NULL REFERENCES patients(patient_id),
    encounter_date  TIMESTAMP NOT NULL,
    org_unit_id     INTEGER REFERENCES org_units(id),
    UNIQUE(patient_id, encounter_date)
);

CREATE INDEX IF NOT EXISTS idx_encounters_patient_id ON encounters(patient_id);
CREATE INDEX IF NOT EXISTS idx_encounters_encounter_date ON encounters(encounter_date);
CREATE INDEX IF NOT EXISTS idx_encounters_org_unit_id ON encounters(org_unit_id);

-- 4. Blood Pressures Table
CREATE TABLE blood_pressures (
    id           BIGSERIAL PRIMARY KEY,
    encounter_id BIGINT NOT NULL REFERENCES encounters(id) ON DELETE CASCADE,
    systolic_bp  NUMERIC,
    diastolic_bp NUMERIC,
    UNIQUE (encounter_id)
);

CREATE INDEX IF NOT EXISTS idx_blood_pressures_encounter_id ON blood_pressures(encounter_id);

-- 5. Blood Sugars Table
CREATE TABLE blood_sugars (
    id                BIGSERIAL PRIMARY KEY,
    encounter_id      BIGINT NOT NULL REFERENCES encounters(id) ON DELETE CASCADE,
    blood_sugar_type  VARCHAR(50) DEFAULT 'RBS',
    blood_sugar_value NUMERIC,
    UNIQUE (encounter_id)
);

CREATE INDEX IF NOT EXISTS idx_blood_sugars_encounter_id ON blood_sugars(encounter_id);

-- 6. Scheduled Visits Table
CREATE TABLE IF NOT EXISTS scheduled_visits (
    scheduled_id   BIGSERIAL PRIMARY KEY,
    patient_id     BIGINT NOT NULL REFERENCES patients(patient_id),
    scheduled_date DATE NOT NULL,
    org_unit_id    INTEGER REFERENCES org_units(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_scheduled_visits_unique ON scheduled_visits(patient_id, scheduled_date);
CREATE INDEX IF NOT EXISTS idx_scheduled_visits_patient_id ON scheduled_visits(patient_id);
CREATE INDEX IF NOT EXISTS idx_scheduled_visits_scheduled_date ON scheduled_visits(scheduled_date);
CREATE INDEX IF NOT EXISTS idx_scheduled_visits_org_unit_id ON scheduled_visits(org_unit_id);

-- 7. Call Results Table
CREATE TABLE IF NOT EXISTS call_results (
    call_id        BIGSERIAL PRIMARY KEY,
    patient_id     BIGINT NOT NULL REFERENCES patients(patient_id),
    call_date      DATE NOT NULL,
    result_type    VARCHAR(255),
    removed_reason VARCHAR(255),
    org_unit_id    INTEGER REFERENCES org_units(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_call_results_unique ON call_results(patient_id, call_date);
CREATE INDEX IF NOT EXISTS idx_call_results_patient_id ON call_results(patient_id);
CREATE INDEX IF NOT EXISTS idx_call_results_call_date ON call_results(call_date);
CREATE INDEX IF NOT EXISTS idx_call_results_org_unit_id ON call_results(org_unit_id);


-- ============================================================================
-- HELPER FUNCTIONS FOR DYNAMIC HIERARCHY
-- ============================================================================

-- Returns all descendant org_unit IDs (including the given ID itself)
CREATE OR REPLACE FUNCTION get_descendant_ids(p_parent_id INTEGER)
RETURNS TABLE(id INTEGER)
LANGUAGE sql STABLE
AS $$
    WITH RECURSIVE descendants AS (
        SELECT ou.id FROM org_units ou WHERE ou.id = p_parent_id
        UNION ALL
        SELECT o.id FROM org_units o JOIN descendants d ON o.parent_id = d.id
    )
    SELECT d.id FROM descendants d;
$$;

-- Returns the ancestor name of a given org_unit at a specific level
CREATE OR REPLACE FUNCTION get_ancestor_name(p_org_unit_id INTEGER, p_target_level INTEGER)
RETURNS VARCHAR
LANGUAGE sql STABLE
AS $$
    WITH RECURSIVE ancestors AS (
        SELECT ou.id, ou.name, ou.level, ou.parent_id
        FROM org_units ou WHERE ou.id = p_org_unit_id
        UNION ALL
        SELECT o.id, o.name, o.level, o.parent_id
        FROM org_units o JOIN ancestors a ON a.parent_id = o.id
    )
    SELECT a.name FROM ancestors a WHERE a.level = p_target_level LIMIT 1;
$$;

-- Upsert a single org_unit and return its ID
CREATE OR REPLACE FUNCTION upsert_org_unit(p_name VARCHAR, p_level INTEGER, p_parent_id INTEGER)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_id INTEGER;
BEGIN
    IF p_parent_id IS NULL THEN
        INSERT INTO org_units (name, level, parent_id)
        VALUES (p_name, p_level, NULL)
        ON CONFLICT (name, level) WHERE parent_id IS NULL
        DO NOTHING;

        SELECT ou.id INTO v_id FROM org_units ou
        WHERE ou.name = p_name AND ou.level = p_level AND ou.parent_id IS NULL;
    ELSE
        INSERT INTO org_units (name, level, parent_id)
        VALUES (p_name, p_level, p_parent_id)
        ON CONFLICT (name, level, parent_id) WHERE parent_id IS NOT NULL
        DO NOTHING;

        SELECT ou.id INTO v_id FROM org_units ou
        WHERE ou.name = p_name AND ou.level = p_level AND ou.parent_id = p_parent_id;
    END IF;

    RETURN v_id;
END;
$$;

-- Upsert an entire hierarchy chain and return the leaf org_unit ID
-- p_names: array of org_unit names from top to bottom
-- p_levels: array of corresponding levels
CREATE OR REPLACE FUNCTION upsert_org_unit_chain(p_names VARCHAR[], p_levels INTEGER[])
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_parent_id INTEGER := NULL;
    v_id INTEGER;
    i INTEGER;
BEGIN
    FOR i IN 1..array_length(p_names, 1) LOOP
        v_id := upsert_org_unit(p_names[i], p_levels[i], v_parent_id);
        v_parent_id := v_id;
    END LOOP;
    RETURN v_id;
END;
$$;


-- Returns the breadcrumb path for a given org_unit as a string like "Region > District > Facility"
CREATE OR REPLACE FUNCTION get_breadcrumb_path(p_org_unit_id INTEGER)
RETURNS TEXT
LANGUAGE sql STABLE
AS $$
    WITH RECURSIVE ancestors AS (
        SELECT ou.id, ou.name, ou.level, ou.parent_id
        FROM org_units ou WHERE ou.id = p_org_unit_id
        UNION ALL
        SELECT o.id, o.name, o.level, o.parent_id
        FROM org_units o JOIN ancestors a ON a.parent_id = o.id
    )
    SELECT string_agg(name, ' > ' ORDER BY level)
    FROM ancestors;
$$;

-- ============================================================================
-- ORG_UNIT_LINEAGE VIEW
-- For each org_unit, lists all its ancestors (including itself).
-- Used by Grafana queries to filter/group by any hierarchy level.
-- ============================================================================
CREATE OR REPLACE VIEW org_unit_lineage AS
WITH RECURSIVE lineage AS (
    SELECT ou.id AS org_unit_id, ou.id AS ancestor_id
    FROM org_units ou
    UNION ALL
    SELECT l.org_unit_id, o.parent_id
    FROM lineage l
    JOIN org_units o ON l.ancestor_id = o.id
    WHERE o.parent_id IS NOT NULL
)
SELECT
    l.org_unit_id,
    l.ancestor_id,
    o.level AS ancestor_level,
    o.name AS ancestor_name
FROM lineage l
JOIN org_units o ON l.ancestor_id = o.id;


-- ============================================================================
-- HIERARCHY_CONFIG: Maps each level to a display name and Grafana variable name.
-- Adding a row here + a matching Grafana variable makes the level appear
-- automatically in drill-down URLs, breadcrumbs, and level labels.
-- ============================================================================
CREATE TABLE IF NOT EXISTS hierarchy_config (
    level        INTEGER PRIMARY KEY,
    display_name VARCHAR(100) NOT NULL,
    var_name     VARCHAR(50)  NOT NULL
);

-- Seed default levels (upsert so re-running is safe)
INSERT INTO hierarchy_config (level, display_name, var_name) VALUES
    (1,  'Region',   'region'),
    (2,  'District', 'district'),
    (3,  'Facility',      'facility'),
    (4,  'Sub-Facility',  'sub_facility'),
    (5,  'Village',  'village'),
    (6,  'Level 6',  'level_6'),
    (7,  'Level 7',  'level_7'),
    (8,  'Level 8',  'level_8'),
    (9,  'Level 9',  'level_9'),
    (10, 'Level 10', 'level_10')
ON CONFLICT (level) DO UPDATE
    SET display_name = EXCLUDED.display_name,
        var_name     = EXCLUDED.var_name;

-- ============================================================================
-- build_drill_url(child_org_unit_id)
-- Dynamically builds a Grafana drill-down URL from the org_unit lineage,
-- using hierarchy_config to map levels to variable names.
-- Works for ANY hierarchy depth — no hard-coded level references.
-- ============================================================================
CREATE OR REPLACE FUNCTION build_drill_url(p_child_id INTEGER)
RETURNS TEXT
LANGUAGE sql STABLE
AS $$
    SELECT '/d/heart360_drilldown?' ||
           string_agg(
               'var-' || hc.var_name || '=' || lin.ancestor_id::text,
               '&' ORDER BY hc.level
           )
    FROM org_unit_lineage lin
    JOIN hierarchy_config hc ON lin.ancestor_level = hc.level
    WHERE lin.org_unit_id = p_child_id;
$$;

-- ============================================================================
-- get_child_level_name(parent_org_unit_id)
-- Returns the display name of the CHILD level for a given org_unit.
-- ============================================================================
CREATE OR REPLACE FUNCTION get_child_level_name(p_org_unit_id INTEGER)
RETURNS TEXT
LANGUAGE sql STABLE
AS $$
    SELECT COALESCE(
        (SELECT CASE WHEN ou.level + 1 > 4 THEN 'Sub-unit'
                     ELSE hc.display_name END
         FROM org_units ou
         LEFT JOIN hierarchy_config hc ON hc.level = ou.level + 1
         WHERE ou.id = p_org_unit_id),
        'Sub-unit'
    );
$$;


-- ============================================================================
-- DROP OLD VIEWS
-- ============================================================================
DROP VIEW IF EXISTS HEART360_PATIENTS_REGISTERED CASCADE;
DROP VIEW IF EXISTS HEART360_PATIENTS_UNDER_CARE CASCADE;
DROP VIEW IF EXISTS HEART360_PATIENTS_CATAGORY CASCADE;
DROP VIEW IF EXISTS HEART360_OVERDUE_PATIENTS CASCADE;
DROP VIEW IF EXISTS HEART360_OVERDUE_START_OF_MONTH CASCADE;
DROP VIEW IF EXISTS HEART360_OVERDUE_PATIENTS_CALLED CASCADE;
DROP VIEW IF EXISTS HEART360_OVERDUE_RETURNED_TO_CARE CASCADE;
DROP VIEW IF EXISTS HEART360_BLOOD_SUGAR_CONTROLLED CASCADE;
DROP VIEW IF EXISTS HEART360_BLOOD_SUGAR_SEVERITY CASCADE;
DROP VIEW IF EXISTS HEART360_BLOOD_SUGAR_MISSED_VISITS CASCADE;
DROP VIEW IF EXISTS HEART360_COHORT_PATIENT_DETAILS CASCADE;


-- ============================================================================
-- VIEW 1: HEART360_PATIENTS_REGISTERED
-- ============================================================================
CREATE OR REPLACE VIEW HEART360_PATIENTS_REGISTERED AS
WITH
KNOWN_MONTHS AS (
  SELECT date_trunc('month', series_date)::date AS REF_MONTH
  FROM generate_series(
      date_trunc('month', (SELECT min(REGISTRATION_DATE) FROM patients)),
      date_trunc('month', current_date),
      '1 month'::interval
  ) AS t(series_date)
),
PATIENTS_BY_MONTH AS (
    SELECT
        DATE_TRUNC('month', REGISTRATION_DATE) AS REF_MONTH,
        p.org_unit_id,
        count(*) AS NB_NEW_PATIENTS
    FROM patients p
    WHERE LOWER(patient_status) <> 'dead'
    GROUP BY DATE_TRUNC('month', REGISTRATION_DATE), p.org_unit_id
)
SELECT
    KNOWN_MONTHS.REF_MONTH,
    REFERENCE_BEFORE.org_unit_id,
    sum(REFERENCE_BEFORE.NB_NEW_PATIENTS) AS CUMULATIVE_NUMBER_OF_PATIENTS,
    sum(CASE WHEN KNOWN_MONTHS.REF_MONTH = REFERENCE_BEFORE.REF_MONTH THEN NB_NEW_PATIENTS ELSE NULL END) AS NB_NEW_PATIENTS
FROM KNOWN_MONTHS
LEFT OUTER JOIN PATIENTS_BY_MONTH REFERENCE_BEFORE
    ON KNOWN_MONTHS.REF_MONTH >= REFERENCE_BEFORE.REF_MONTH
GROUP BY 1, 2
ORDER BY KNOWN_MONTHS.REF_MONTH DESC;


-- ============================================================================
-- VIEW 2: HEART360_PATIENTS_UNDER_CARE
-- ============================================================================
CREATE OR REPLACE VIEW HEART360_PATIENTS_UNDER_CARE AS
WITH
KNOWN_MONTHS AS (
  SELECT date_trunc('month', series_date)::date AS REF_MONTH
  FROM generate_series(
      date_trunc('month', (SELECT min(REGISTRATION_DATE) FROM patients)),
      date_trunc('month', current_date),
      '1 month'::interval
  ) AS t(series_date)
),
ALIVE_PATIENTS AS (
    SELECT
        DATE_TRUNC('month', REGISTRATION_DATE) AS REGISTRATION_MONTH,
        p.org_unit_id,
        p.patient_id
    FROM patients p
    WHERE LOWER(patient_status) <> 'dead'
),
BP_ENCOUNTERS AS (
    SELECT e.patient_id,
           DATE_TRUNC('month', e.encounter_date) AS BP_ENCOUNTER_MONTH
    FROM encounters e
    JOIN blood_pressures bp ON bp.encounter_id = e.id
)
SELECT
    KNOWN_MONTHS.REF_MONTH,
    ALIVE_PATIENTS.org_unit_id,
    sum(CASE WHEN BP_ENCOUNTERS.patient_id IS NULL THEN 1 ELSE NULL END) AS NB_PATIENTS_LOST_TO_FOLLOW_UP,
    count(DISTINCT BP_ENCOUNTERS.patient_id) AS NB_PATIENTS_UNDER_CARE,
    count(DISTINCT ALIVE_PATIENTS.patient_id) AS CUMULATIVE_NUMBER_OF_PATIENTS
FROM KNOWN_MONTHS
LEFT OUTER JOIN ALIVE_PATIENTS
    ON ALIVE_PATIENTS.REGISTRATION_MONTH <= KNOWN_MONTHS.REF_MONTH
LEFT OUTER JOIN BP_ENCOUNTERS
    ON BP_ENCOUNTERS.patient_id = ALIVE_PATIENTS.patient_id
        AND BP_ENCOUNTER_MONTH <= KNOWN_MONTHS.REF_MONTH
        AND BP_ENCOUNTER_MONTH + interval '12 month' > KNOWN_MONTHS.REF_MONTH
GROUP BY 1, 2
ORDER BY KNOWN_MONTHS.REF_MONTH DESC;


-- ============================================================================
-- VIEW 3: HEART360_PATIENTS_CATAGORY
-- ============================================================================
CREATE OR REPLACE VIEW HEART360_PATIENTS_CATAGORY AS
WITH
KNOWN_MONTHS AS (
  SELECT date_trunc('month', series_date)::date AS REF_MONTH
  FROM generate_series(
      date_trunc('month', (SELECT min(REGISTRATION_DATE) FROM patients)),
      date_trunc('month', current_date),
      '1 month'::interval
  ) AS t(series_date)
),
ALIVE_PATIENTS AS (
    SELECT
        DATE_TRUNC('month', REGISTRATION_DATE) AS REGISTRATION_MONTH,
        p.org_unit_id,
        p.patient_id AS patient_id
    FROM patients p
    WHERE LOWER(patient_status) <> 'dead'
),
BP_ENCOUNTERS AS (
    SELECT
        e.id AS id,
        e.patient_id,
        bp.systolic_bp AS systolic,
        bp.diastolic_bp AS diastolic,
        e.encounter_date AS BP_ENCOUNTER_DATE,
        DATE_TRUNC('month', e.encounter_date) AS BP_ENCOUNTER_MONTH
    FROM encounters e
    LEFT JOIN blood_pressures bp ON e.id = bp.encounter_id
),
LATEST_BP_BY_MONTH_AND_PATIENT AS (
    WITH MOST_RECENT_BP_ENCOUNTER AS (
        SELECT
            KNOWN_MONTHS.REF_MONTH,
            e.patient_id,
            MAX(e.encounter_date) AS MOST_RECENT_BP_DATE
        FROM encounters e
        JOIN blood_pressures bp ON bp.encounter_id = e.id
        JOIN KNOWN_MONTHS ON DATE_TRUNC('month', e.encounter_date) <= KNOWN_MONTHS.REF_MONTH
        GROUP BY KNOWN_MONTHS.REF_MONTH, e.patient_id
    )
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
    ALIVE_PATIENTS.org_unit_id,
    count(*) AS TOTAL_NUMBER_OF_PATIENTS,
    SUM(CASE WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH IS NULL THEN 0 WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH + interval '12 month' <= KNOWN_MONTHS.REF_MONTH THEN 0 ELSE 1 END) AS NB_PATIENTS_UNDER_CARE,
    SUM(CASE WHEN ALIVE_PATIENTS.REGISTRATION_MONTH + interval '3 month' > KNOWN_MONTHS.REF_MONTH THEN 1 ELSE 0 END) AS NB_PATIENTS_NEWLY_REGISTERED,
    SUM(CASE
        WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH IS NULL THEN 0
        WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH + interval '12 month' <= KNOWN_MONTHS.REF_MONTH THEN 0
        WHEN ALIVE_PATIENTS.REGISTRATION_MONTH + interval '3 month' > KNOWN_MONTHS.REF_MONTH THEN 0 ELSE 1 END
    ) AS NB_PATIENTS_UNDER_CARE_REGISTERED_BEFORE_THE_PAST_3_MONTHS,
    SUM(CASE WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH IS NULL THEN 1 WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH + interval '12 month' <= KNOWN_MONTHS.REF_MONTH THEN 1 ELSE 0 END) AS NB_PATIENTS_LOST_TO_FOLLOW_UP,
    SUM(CASE
        WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH IS NULL THEN 0
        WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH + interval '12 month' <= KNOWN_MONTHS.REF_MONTH THEN 0
        WHEN ALIVE_PATIENTS.REGISTRATION_MONTH + interval '3 month' > KNOWN_MONTHS.REF_MONTH THEN 0
        WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH + interval '3 month' <= KNOWN_MONTHS.REF_MONTH THEN 1
        ELSE 0 END) AS NB_PATIENTS_NO_VISIT,
    SUM(CASE
        WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH IS NULL THEN 0
        WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH + interval '12 month' <= KNOWN_MONTHS.REF_MONTH THEN 0
        WHEN ALIVE_PATIENTS.REGISTRATION_MONTH + interval '3 month' > KNOWN_MONTHS.REF_MONTH THEN 0
        WHEN systolic > 140 OR diastolic > 90 THEN 1
        ELSE 0 END) AS NB_PATIENTS_UNCONTROLLED,
    SUM(CASE
        WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH IS NULL THEN 0
        WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH + interval '3 month' <= KNOWN_MONTHS.REF_MONTH THEN 0
        WHEN ALIVE_PATIENTS.REGISTRATION_MONTH + interval '3 month' > KNOWN_MONTHS.REF_MONTH THEN 0
        WHEN systolic IS NULL OR diastolic IS NULL THEN 0
        WHEN systolic > 140 OR diastolic > 90 THEN 0
        ELSE 1 END) AS NB_PATIENTS_CONTROLLED,
    SUM(CASE
        WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH IS NULL THEN 0
        WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH + interval '12 month' <= KNOWN_MONTHS.REF_MONTH THEN 0
        WHEN ALIVE_PATIENTS.REGISTRATION_MONTH + interval '3 month' > KNOWN_MONTHS.REF_MONTH THEN 0
        WHEN LATEST_BP_BY_MONTH_AND_PATIENT.BP_ENCOUNTER_MONTH + interval '3 month' <= KNOWN_MONTHS.REF_MONTH THEN 0
        WHEN systolic IS NULL OR diastolic IS NULL THEN 1
        ELSE 0 END) AS NB_PATIENTS_VISIT_NO_BP
FROM KNOWN_MONTHS
LEFT OUTER JOIN ALIVE_PATIENTS
    ON ALIVE_PATIENTS.REGISTRATION_MONTH <= KNOWN_MONTHS.REF_MONTH
LEFT OUTER JOIN LATEST_BP_BY_MONTH_AND_PATIENT
    ON LATEST_BP_BY_MONTH_AND_PATIENT.patient_id = ALIVE_PATIENTS.patient_id
    AND LATEST_BP_BY_MONTH_AND_PATIENT.REF_MONTH = KNOWN_MONTHS.REF_MONTH
GROUP BY 1, 2
ORDER BY 1 DESC;


-- ============================================================================
-- VIEW 4: HEART360_OVERDUE_PATIENTS
-- ============================================================================
CREATE OR REPLACE VIEW HEART360_OVERDUE_PATIENTS AS
WITH MOST_RECENT_ENCOUNTER AS (
    SELECT DISTINCT ON (e.patient_id)
        e.patient_id,
        e.encounter_date,
        bp.systolic_bp,
        bp.diastolic_bp
    FROM encounters e
    LEFT JOIN blood_pressures bp ON e.id = bp.encounter_id
    WHERE (e.patient_id, e.encounter_date) IN (
        SELECT patient_id, MAX(encounter_date)
        FROM encounters
        GROUP BY patient_id
    )
    ORDER BY e.patient_id, e.encounter_date DESC, e.id DESC, COALESCE(bp.id, 0) DESC
),
MOST_RECENT_CALL AS (
    SELECT DISTINCT ON (patient_id) *
    FROM call_results
    WHERE (patient_id, call_date) IN (
        SELECT patient_id, MAX(call_date)
        FROM call_results
        GROUP BY patient_id
    )
    ORDER BY patient_id, call_date DESC, call_id DESC
)
SELECT
    p.patient_id,
    p.patient_name,
    p.registration_date,
    p.birth_date,
    p.gender,
    p.phone_number,
    p.org_unit_id,
    mre.encounter_date AS last_visit_date,
    mre.diastolic_bp AS last_bp_diastolic,
    mre.systolic_bp AS last_bp_systolic,
    mrc.call_date::TIMESTAMP AS last_call_date,
    mrc.result_type AS last_call_result,
    mrc.removed_reason
FROM patients p
LEFT JOIN MOST_RECENT_ENCOUNTER mre ON p.patient_id = mre.patient_id
LEFT JOIN MOST_RECENT_CALL mrc
    ON p.patient_id = mrc.patient_id
    AND (mre.encounter_date IS NULL OR mrc.call_date::TIMESTAMP > mre.encounter_date)
WHERE (mrc.result_type IS NULL OR LOWER(mrc.result_type) <> 'removed_from_overdue_list');


-- ============================================================================
-- VIEW 5: HEART360_COHORT_PATIENT_DETAILS
-- ============================================================================
CREATE OR REPLACE VIEW HEART360_COHORT_PATIENT_DETAILS AS
WITH patients_quarter AS (
    SELECT
        p.patient_id,
        p.org_unit_id,
        date_trunc('quarter', registration_date) AS registration_quarter,
        date_trunc('quarter', registration_date) + interval '6 month' AS cohort_validation_month,
        registration_date
    FROM patients p
),
LAST_BP_IN_INTERVAL AS (
    SELECT
        e.patient_id,
        e.encounter_date,
        bp.systolic_bp,
        bp.diastolic_bp
    FROM encounters e
    LEFT JOIN blood_pressures bp ON e.id = bp.encounter_id
    WHERE e.id IN (
        SELECT max(e2.id) AS encounter_id
        FROM encounters e2
        WHERE (e2.patient_id, e2.encounter_date) IN (
            SELECT
                patients_quarter.patient_id,
                max(e3.encounter_date) AS most_recent_bp
            FROM encounters e3
            JOIN patients_quarter
                ON patients_quarter.patient_id = e3.patient_id
                AND e3.encounter_date < patients_quarter.cohort_validation_month
            GROUP BY patients_quarter.patient_id
        )
        GROUP BY e2.patient_id
    )
)
SELECT
    patients_quarter.patient_id,
    patients_quarter.org_unit_id,
    registration_quarter,
    CASE
        WHEN encounter_date IS NULL THEN 'missed visit'
        WHEN cohort_validation_month > encounter_date + interval '3 month' THEN 'missed visit'
        WHEN diastolic_bp < 90 AND systolic_bp < 140 THEN 'controlled'
        ELSE 'uncontrolled'
    END AS status_at_end_of_interval
FROM patients_quarter
LEFT OUTER JOIN LAST_BP_IN_INTERVAL ON LAST_BP_IN_INTERVAL.patient_id = patients_quarter.patient_id;


-- ============================================================================
-- VIEW 6: HEART360_OVERDUE_START_OF_MONTH
-- ============================================================================
CREATE OR REPLACE VIEW HEART360_OVERDUE_START_OF_MONTH AS
WITH REF_MONTHS AS (
  SELECT generate_series(
      date_trunc('month', (SELECT MIN(registration_date) FROM patients)),
      date_trunc('month', CURRENT_DATE),
      interval '1 month'
  )::date AS ref_month
),
PATIENTS_UNDER_CARE AS (
  SELECT rm.ref_month, p.patient_id
  FROM REF_MONTHS rm
  JOIN patients p
    ON p.registration_date <= rm.ref_month - INTERVAL '3 months'
   AND p.patient_status = 'ALIVE'
   AND p.death_date IS NULL
  WHERE EXISTS (
      SELECT 1 FROM encounters be
      WHERE be.patient_id = p.patient_id
        AND be.encounter_date >= rm.ref_month - INTERVAL '12 months'
        AND be.encounter_date < rm.ref_month
  )
),
LATEST_SCHEDULED AS (
  SELECT rm.ref_month, sv.patient_id, MAX(sv.scheduled_date) AS scheduled_date
  FROM REF_MONTHS rm
  JOIN scheduled_visits sv ON sv.scheduled_date < rm.ref_month
  GROUP BY rm.ref_month, sv.patient_id
),
RETURNED_BEFORE_MONTH AS (
  SELECT DISTINCT ls.ref_month, ls.patient_id
  FROM LATEST_SCHEDULED ls
  JOIN encounters be
    ON be.patient_id = ls.patient_id
   AND be.encounter_date >= ls.scheduled_date
   AND be.encounter_date < ls.ref_month
),
REMOVED_BEFORE_MONTH AS (
  SELECT DISTINCT rm.ref_month, cr.patient_id
  FROM REF_MONTHS rm
  JOIN call_results cr ON cr.call_date < rm.ref_month
  WHERE LOWER(cr.result_type) = 'removed_from_overdue_list'
)
SELECT
  ls.ref_month,
  p.org_unit_id,
  COUNT(DISTINCT ls.patient_id) AS overdue_on_first
FROM LATEST_SCHEDULED ls
JOIN PATIENTS_UNDER_CARE puc
  ON puc.patient_id = ls.patient_id AND puc.ref_month = ls.ref_month
JOIN patients p ON p.patient_id = ls.patient_id
LEFT JOIN RETURNED_BEFORE_MONTH rbm
  ON rbm.patient_id = ls.patient_id AND rbm.ref_month = ls.ref_month
LEFT JOIN REMOVED_BEFORE_MONTH rmb
  ON rmb.patient_id = ls.patient_id AND rmb.ref_month = ls.ref_month
WHERE p.phone_number IS NOT NULL
  AND LENGTH(REGEXP_REPLACE(p.phone_number, '[^0-9]', '', 'g')) >= 8
  AND rbm.patient_id IS NULL
  AND rmb.patient_id IS NULL
GROUP BY ls.ref_month, p.org_unit_id
ORDER BY ls.ref_month;


-- ============================================================================
-- VIEW 7: HEART360_OVERDUE_PATIENTS_CALLED
-- ============================================================================
CREATE OR REPLACE VIEW HEART360_OVERDUE_PATIENTS_CALLED AS
WITH FIRST_CALLS AS (
  SELECT DISTINCT ON (cr.patient_id, date_trunc('month', cr.call_date))
    cr.patient_id,
    date_trunc('month', cr.call_date)::date AS ref_month,
    cr.call_date,
    cr.result_type,
    cr.removed_reason
  FROM call_results cr
  ORDER BY cr.patient_id, date_trunc('month', cr.call_date), cr.call_date ASC
)
SELECT
  fc.ref_month,
  p.org_unit_id,
  COUNT(DISTINCT fc.patient_id) AS overdue_patients_called
FROM FIRST_CALLS fc
JOIN patients p ON p.patient_id = fc.patient_id
WHERE p.patient_status = 'ALIVE'
  AND NOT (
    LOWER(fc.result_type) = 'removed_from_overdue_list'
    AND LOWER(fc.removed_reason) = 'died'
  )
GROUP BY fc.ref_month, p.org_unit_id
ORDER BY fc.ref_month;


-- ============================================================================
-- VIEW 8: HEART360_OVERDUE_RETURNED_TO_CARE
-- ============================================================================
CREATE OR REPLACE VIEW HEART360_OVERDUE_RETURNED_TO_CARE AS
WITH FIRST_CALLS AS (
    SELECT DISTINCT ON (cr.patient_id, date_trunc('month', cr.call_date))
        cr.patient_id,
        date_trunc('month', cr.call_date)::date AS ref_month,
        cr.call_date,
        cr.call_date + INTERVAL '15 days' AS window_end
    FROM call_results cr
    ORDER BY cr.patient_id, date_trunc('month', cr.call_date), cr.call_date ASC
),
RETURNED AS (
    SELECT DISTINCT fc.patient_id, fc.ref_month
    FROM FIRST_CALLS fc
    JOIN encounters be
      ON be.patient_id = fc.patient_id
     AND be.encounter_date >= fc.call_date
     AND be.encounter_date <= fc.window_end
)
SELECT
    r.ref_month,
    p.org_unit_id,
    COUNT(DISTINCT r.patient_id) AS overdue_returned_to_care
FROM RETURNED r
JOIN patients p ON p.patient_id = r.patient_id
WHERE p.patient_status = 'ALIVE'
GROUP BY r.ref_month, p.org_unit_id
ORDER BY r.ref_month;


-- ============================================================================
-- VIEW 9: HEART360_BLOOD_SUGAR_CONTROLLED
-- ============================================================================
CREATE OR REPLACE VIEW HEART360_BLOOD_SUGAR_CONTROLLED AS
WITH REF_MONTHS AS (
    SELECT generate_series(
        date_trunc('month', (SELECT MIN(registration_date) FROM patients)),
        date_trunc('month', CURRENT_DATE),
        interval '1 month'
    )::date AS ref_month
),
ALL_PATIENTS AS (
    SELECT p.patient_id, p.org_unit_id, p.registration_date, p.death_date
    FROM patients p
),
BS_ENCOUNTERS AS (
    SELECT e.patient_id, e.encounter_date, bs.blood_sugar_type, bs.blood_sugar_value
    FROM encounters e
    JOIN blood_sugars bs ON bs.encounter_id = e.id
),
LATEST_BS AS (
    SELECT rm.ref_month, e.patient_id, MAX(e.encounter_date) AS latest_bs_date
    FROM REF_MONTHS rm
    JOIN BS_ENCOUNTERS e ON DATE_TRUNC('month', e.encounter_date) <= rm.ref_month
    GROUP BY rm.ref_month, e.patient_id
),
LATEST_BS_VALUES AS (
    SELECT lb.ref_month, e.patient_id, e.encounter_date, e.blood_sugar_type, e.blood_sugar_value
    FROM LATEST_BS lb
    JOIN BS_ENCOUNTERS e
        ON lb.patient_id = e.patient_id AND lb.latest_bs_date = e.encounter_date
)
SELECT
    rm.ref_month,
    p.org_unit_id,
    COUNT(DISTINCT p.patient_id) FILTER (
        WHERE p.registration_date <= rm.ref_month - interval '3 months'
            AND EXISTS (
                SELECT 1 FROM encounters e
                WHERE e.patient_id = p.patient_id
                AND DATE_TRUNC('month', e.encounter_date) <= rm.ref_month
                AND DATE_TRUNC('month', e.encounter_date) + interval '12 month' > rm.ref_month
            )
    ) AS diabetes_patients_under_care,
    COUNT(DISTINCT p.patient_id) FILTER (
        WHERE p.registration_date <= rm.ref_month - interval '3 months'
            AND DATE_TRUNC('month', lbv.encounter_date) + interval '3 month' > rm.ref_month
            AND (
                (LOWER(lbv.blood_sugar_type) IN ('rbs', 'random') AND lbv.blood_sugar_value < 140)
                OR (LOWER(lbv.blood_sugar_type) IN ('fbs', 'fasting') AND lbv.blood_sugar_value < 126)
                OR (LOWER(lbv.blood_sugar_type) = 'hba1c' AND lbv.blood_sugar_value < 7)
            )
    ) AS diabetes_controlled
FROM REF_MONTHS rm
LEFT JOIN ALL_PATIENTS p
    ON p.registration_date <= rm.ref_month
    AND (p.death_date IS NULL OR DATE_TRUNC('month', p.death_date) >= rm.ref_month)
LEFT JOIN LATEST_BS_VALUES lbv
    ON lbv.patient_id = p.patient_id AND lbv.ref_month = rm.ref_month
GROUP BY rm.ref_month, p.org_unit_id
ORDER BY rm.ref_month;


-- ============================================================================
-- VIEW 10: HEART360_BLOOD_SUGAR_SEVERITY
-- ============================================================================
CREATE OR REPLACE VIEW HEART360_BLOOD_SUGAR_SEVERITY AS
WITH KNOWN_MONTHS AS (
  SELECT date_trunc('month', series_date)::date AS ref_month
  FROM generate_series(
      (SELECT min(registration_date) FROM patients),
      date_trunc('month', current_date) + interval '1 month',
      interval '1 month'
  ) AS t(series_date)
),
ALL_PATIENTS AS (
  SELECT p.patient_id, p.org_unit_id, p.registration_date, p.death_date
  FROM patients p
),
BS_ENCOUNTERS AS (
  SELECT e.patient_id, e.encounter_date, bs.blood_sugar_type, bs.blood_sugar_value
  FROM encounters e
  JOIN blood_sugars bs ON bs.encounter_id = e.id
),
LATEST_BS AS (
  SELECT km.ref_month, e.patient_id, MAX(e.encounter_date) AS latest_bs_date
  FROM KNOWN_MONTHS km
  JOIN BS_ENCOUNTERS e ON DATE_TRUNC('month', e.encounter_date) <= km.ref_month
  GROUP BY km.ref_month, e.patient_id
),
LATEST_BS_VALUES AS (
  SELECT lb.ref_month, e.patient_id, e.encounter_date, e.blood_sugar_type, e.blood_sugar_value
  FROM LATEST_BS lb
  JOIN BS_ENCOUNTERS e ON lb.patient_id = e.patient_id AND lb.latest_bs_date = e.encounter_date
)
SELECT
  km.ref_month,
  p.org_unit_id,
  COUNT(DISTINCT p.patient_id) FILTER (
    WHERE p.registration_date <= km.ref_month - interval '3 month'
      AND (p.registration_date >= km.ref_month - interval '12 month'
        OR EXISTS (
            SELECT 1 FROM encounters e
            WHERE e.patient_id = p.patient_id
              AND DATE_TRUNC('month', e.encounter_date) <= km.ref_month
              AND DATE_TRUNC('month', e.encounter_date) + interval '12 month' > km.ref_month
        ))
  ) AS diabetes_patients_under_care,
  COUNT(DISTINCT p.patient_id) FILTER (
    WHERE p.registration_date <= km.ref_month - interval '3 month'
      AND (p.registration_date >= km.ref_month - interval '12 month'
        OR EXISTS (
            SELECT 1 FROM encounters e
            WHERE e.patient_id = p.patient_id
              AND DATE_TRUNC('month', e.encounter_date) <= km.ref_month
              AND DATE_TRUNC('month', e.encounter_date) + interval '12 month' > km.ref_month
        ))
      AND DATE_TRUNC('month', lbv.encounter_date) + interval '3 month' > km.ref_month
      AND (
           (LOWER(lbv.blood_sugar_type) IN ('rbs', 'random') AND lbv.blood_sugar_value >= 140 AND lbv.blood_sugar_value <= 199)
        OR (LOWER(lbv.blood_sugar_type) IN ('fbs', 'fasting') AND lbv.blood_sugar_value >= 126 AND lbv.blood_sugar_value <= 199)
        OR (LOWER(lbv.blood_sugar_type) = 'hba1c' AND lbv.blood_sugar_value >= 7 AND lbv.blood_sugar_value <= 8.9)
      )
  ) AS uncontrolled_moderate,
  COUNT(DISTINCT p.patient_id) FILTER (
    WHERE p.registration_date <= km.ref_month - interval '3 month'
      AND (p.registration_date >= km.ref_month - interval '12 month'
        OR EXISTS (
            SELECT 1 FROM encounters e
            WHERE e.patient_id = p.patient_id
              AND DATE_TRUNC('month', e.encounter_date) <= km.ref_month
              AND DATE_TRUNC('month', e.encounter_date) + interval '12 month' > km.ref_month
        ))
      AND DATE_TRUNC('month', lbv.encounter_date) + interval '3 month' > km.ref_month
      AND (
           (LOWER(lbv.blood_sugar_type) IN ('rbs', 'random') AND lbv.blood_sugar_value >= 200)
        OR (LOWER(lbv.blood_sugar_type) IN ('fbs', 'fasting') AND lbv.blood_sugar_value >= 200)
        OR (LOWER(lbv.blood_sugar_type) = 'hba1c' AND lbv.blood_sugar_value >= 9)
      )
  ) AS uncontrolled_high
FROM KNOWN_MONTHS km
LEFT JOIN ALL_PATIENTS p
  ON p.registration_date <= km.ref_month
  AND (p.death_date IS NULL OR DATE_TRUNC('month', p.death_date) >= km.ref_month)
LEFT JOIN LATEST_BS_VALUES lbv
  ON lbv.patient_id = p.patient_id AND lbv.ref_month = km.ref_month
GROUP BY km.ref_month, p.org_unit_id
ORDER BY km.ref_month;


-- ============================================================================
-- VIEW 11: HEART360_BLOOD_SUGAR_MISSED_VISITS
-- ============================================================================
CREATE OR REPLACE VIEW HEART360_BLOOD_SUGAR_MISSED_VISITS AS
WITH KNOWN_MONTHS AS (
  SELECT date_trunc('month', series_date)::date AS ref_month
  FROM generate_series(
      (SELECT min(registration_date) FROM patients),
      date_trunc('month', current_date),
      interval '1 month'
  ) AS t(series_date)
),
ALL_PATIENTS AS (
  SELECT p.patient_id, p.org_unit_id, p.registration_date, p.death_date
  FROM patients p
),
LAST_VISIT_BEFORE_MONTH AS (
  SELECT km.ref_month, e.patient_id, MAX(e.encounter_date) AS last_visit_date
  FROM KNOWN_MONTHS km
  JOIN encounters e ON DATE_TRUNC('month', e.encounter_date) <= km.ref_month
  GROUP BY km.ref_month, e.patient_id
)
SELECT
  km.ref_month,
  p.org_unit_id,
  COUNT(DISTINCT p.patient_id) FILTER (
    WHERE p.registration_date <= km.ref_month - interval '3 month'
      AND (p.registration_date >= km.ref_month - interval '12 month'
        OR EXISTS (
            SELECT 1 FROM encounters e
            WHERE e.patient_id = p.patient_id
              AND DATE_TRUNC('month', e.encounter_date) <= km.ref_month
              AND DATE_TRUNC('month', e.encounter_date) + interval '12 month' > km.ref_month
        ))
  ) AS diabetes_patients_under_care,
  COUNT(DISTINCT p.patient_id) FILTER (
    WHERE p.registration_date <= km.ref_month - interval '3 month'
      AND (p.registration_date >= km.ref_month - interval '12 month'
        OR EXISTS (
            SELECT 1 FROM encounters e
            WHERE e.patient_id = p.patient_id
              AND DATE_TRUNC('month', e.encounter_date) <= km.ref_month
              AND DATE_TRUNC('month', e.encounter_date) + interval '12 month' > km.ref_month
        ))
      AND (lv.last_visit_date IS NULL OR DATE_TRUNC('month', lv.last_visit_date) + interval '3 month' <= km.ref_month)
  ) AS missed_visit
FROM KNOWN_MONTHS km
LEFT JOIN ALL_PATIENTS p
  ON p.registration_date <= km.ref_month
  AND (p.death_date IS NULL OR DATE_TRUNC('month', p.death_date) >= km.ref_month)
LEFT JOIN LAST_VISIT_BEFORE_MONTH lv
  ON lv.patient_id = p.patient_id AND lv.ref_month = km.ref_month
GROUP BY km.ref_month, p.org_unit_id
ORDER BY km.ref_month;


-- ============================================================================
-- TRIGGER: Update patient status when call_results indicates death
-- ============================================================================
CREATE OR REPLACE FUNCTION update_patient_status_on_death()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF LOWER(TRIM(COALESCE(NEW.result_type, ''))) = 'removed_from_overdue_list'
        AND LOWER(TRIM(COALESCE(NEW.removed_reason, ''))) = 'died' THEN
        UPDATE patients
        SET patient_status = 'DEAD',
            death_date = COALESCE(death_date, NEW.call_date)
        WHERE patient_id = NEW.patient_id
          AND patient_status = 'ALIVE';
        RAISE NOTICE 'Patient % marked as DEAD', NEW.patient_id;
    END IF;
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trigger_update_patient_status_on_death ON reminder_calls;
DROP TRIGGER IF EXISTS trigger_update_patient_status_on_death ON call_results;

CREATE TRIGGER trigger_update_patient_status_on_death
    AFTER INSERT OR UPDATE ON call_results
    FOR EACH ROW
    EXECUTE FUNCTION update_patient_status_on_death();

-- Drop deprecated tables
DROP TABLE IF EXISTS reminder_calls CASCADE;
DROP TABLE IF EXISTS facilities CASCADE;


-- ============================================================================
-- ADMIN PROCEDURE: H360TK_ADMIN_CLEAN_DATA
-- Clears all patient and organizational data for a fresh start.
-- Usage:  CALL heart360tk_schema.H360TK_ADMIN_CLEAN_DATA();
-- ============================================================================
CREATE OR REPLACE PROCEDURE H360TK_ADMIN_CLEAN_DATA()
LANGUAGE plpgsql AS $$
BEGIN
    SET search_path TO heart360tk_schema;

    TRUNCATE TABLE
        blood_pressures,
        blood_sugars,
        scheduled_visits,
        call_results,
        encounters,
        patients,
        org_units
    CASCADE;

    ALTER SEQUENCE org_units_id_seq RESTART WITH 1;
    ALTER SEQUENCE encounters_id_seq RESTART WITH 1;
    ALTER SEQUENCE blood_pressures_id_seq RESTART WITH 1;
    ALTER SEQUENCE blood_sugars_id_seq RESTART WITH 1;
    ALTER SEQUENCE scheduled_visits_scheduled_id_seq RESTART WITH 1;
    ALTER SEQUENCE call_results_call_id_seq RESTART WITH 1;

    RAISE NOTICE 'All data cleared and sequences reset.';
END;
$$;
