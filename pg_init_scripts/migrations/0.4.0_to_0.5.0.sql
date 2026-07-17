BEGIN;

SET ROLE heart360tk;

SET search_path TO heart360tk_schema;
-- ============================================================================
-- STEP 1: Convert the 13 reporting materialized views into empty tables.
--         Drop the matview only if the object is currently a materialized view
--         (so re-running after a partial migration, when it is already a table,
--         is a no-op and never errors).
-- ============================================================================

DO $$
DECLARE
    v_name text;
    v_matviews text[] := ARRAY[
        'heart360_patients_category',
        'heart360_patients_under_care',
        'heart360_patients_registered',
        'heart360_blood_sugar_controlled',
        'heart360_blood_sugar_severity',
        'heart360_blood_sugar_missed_visits',
        'heart360_dm_bp_control',
        'heart360_dm_patients_under_care',
        'heart360_overdue_patients',
        'heart360_overdue_start_of_month',
        'heart360_overdue_patients_called',
        'heart360_overdue_returned_to_care',
        'heart360_cohort_patient_details'
    ];
BEGIN
    FOREACH v_name IN ARRAY v_matviews
    LOOP
        IF EXISTS (
            SELECT 1 FROM pg_matviews
            WHERE schemaname = 'heart360tk_reporting'
              AND matviewname = v_name
        ) THEN
            EXECUTE format('DROP MATERIALIZED VIEW IF EXISTS heart360tk_reporting.%I CASCADE', v_name);
        END IF;
    END LOOP;
END $$;

-- ============================================================================
-- STEP 2: Create the reporting tables (empty; structure inherited from the
--         source schema views). Includes the new DM_PATIENTS_CATAGORY table.
-- ============================================================================

CREATE TABLE IF NOT EXISTS heart360tk_reporting.HEART360_PATIENTS_CATEGORY
    AS SELECT * FROM heart360tk_schema.HEART360_PATIENTS_CATEGORY WHERE 1=0;
CREATE TABLE IF NOT EXISTS heart360tk_reporting.HEART360_PATIENTS_UNDER_CARE
    AS SELECT * FROM heart360tk_schema.HEART360_PATIENTS_UNDER_CARE WHERE 1=0;
CREATE TABLE IF NOT EXISTS heart360tk_reporting.HEART360_PATIENTS_REGISTERED
    AS SELECT * FROM heart360tk_schema.HEART360_PATIENTS_REGISTERED WHERE 1=0;
CREATE TABLE IF NOT EXISTS heart360tk_reporting.HEART360_BLOOD_SUGAR_CONTROLLED
    AS SELECT * FROM heart360tk_schema.HEART360_BLOOD_SUGAR_CONTROLLED WHERE 1=0;
CREATE TABLE IF NOT EXISTS heart360tk_reporting.HEART360_BLOOD_SUGAR_SEVERITY
    AS SELECT * FROM heart360tk_schema.HEART360_BLOOD_SUGAR_SEVERITY WHERE 1=0;
CREATE TABLE IF NOT EXISTS heart360tk_reporting.HEART360_BLOOD_SUGAR_MISSED_VISITS
    AS SELECT * FROM heart360tk_schema.HEART360_BLOOD_SUGAR_MISSED_VISITS WHERE 1=0;
CREATE TABLE IF NOT EXISTS heart360tk_reporting.HEART360_DM_BP_CONTROL
    AS SELECT * FROM heart360tk_schema.HEART360_DM_BP_CONTROL WHERE 1=0;
CREATE TABLE IF NOT EXISTS heart360tk_reporting.HEART360_DM_PATIENTS_UNDER_CARE
    AS SELECT * FROM heart360tk_schema.HEART360_DM_PATIENTS_UNDER_CARE WHERE 1=0;
CREATE TABLE IF NOT EXISTS heart360tk_reporting.HEART360_OVERDUE_PATIENTS
    AS SELECT * FROM heart360tk_schema.HEART360_OVERDUE_PATIENTS WHERE 1=0;
CREATE TABLE IF NOT EXISTS heart360tk_reporting.HEART360_OVERDUE_START_OF_MONTH
    AS SELECT * FROM heart360tk_schema.HEART360_OVERDUE_START_OF_MONTH WHERE 1=0;
CREATE TABLE IF NOT EXISTS heart360tk_reporting.HEART360_OVERDUE_PATIENTS_CALLED
    AS SELECT * FROM heart360tk_schema.HEART360_OVERDUE_PATIENTS_CALLED WHERE 1=0;
CREATE TABLE IF NOT EXISTS heart360tk_reporting.HEART360_OVERDUE_RETURNED_TO_CARE
    AS SELECT * FROM heart360tk_schema.HEART360_OVERDUE_RETURNED_TO_CARE WHERE 1=0;
CREATE TABLE IF NOT EXISTS heart360tk_reporting.HEART360_COHORT_PATIENT_DETAILS
    AS SELECT * FROM heart360tk_schema.HEART360_COHORT_PATIENT_DETAILS WHERE 1=0;

-- New in 0.5.0.
CREATE TABLE IF NOT EXISTS heart360tk_reporting.HEART360_DM_PATIENTS_CATAGORY
    AS SELECT * FROM heart360tk_schema.HEART360_DM_PATIENTS_CATAGORY WHERE 1=0;

-- ============================================================================
-- STEP 3: New leaf<->central org-unit mapping table (new in 0.5.0).
-- ============================================================================

CREATE TABLE IF NOT EXISTS heart360tk_reporting.IMPORT_FACILITY_MAPPING (
    leaf_node_key       character varying(255),
    leaf_org_unit_id    integer,
    central_org_unit_id integer,
    last_updated_date   timestamp,
    last_extract_date   timestamp
);

CREATE INDEX IF NOT EXISTS idx_import_facility_mapping_leaf_node_key
    ON heart360tk_reporting.IMPORT_FACILITY_MAPPING (leaf_node_key);

-- ============================================================================
-- STEP 4: Indexes on the reporting tables.
--         idx_cohort_patient_id is intentionally NOT recreated (removed in
--         0.5.0); drop it defensively in case a prior run left it behind.
-- ============================================================================

DROP INDEX IF EXISTS heart360tk_reporting.idx_cohort_patient_id;

CREATE UNIQUE INDEX IF NOT EXISTS idx_pat_cat_org_month
    ON heart360tk_reporting.HEART360_PATIENTS_CATEGORY (org_unit_id, ref_month);
CREATE UNIQUE INDEX IF NOT EXISTS idx_pat_under_care_org_month
    ON heart360tk_reporting.HEART360_PATIENTS_UNDER_CARE (org_unit_id, ref_month);
CREATE UNIQUE INDEX IF NOT EXISTS idx_pat_registered_org_month
    ON heart360tk_reporting.HEART360_PATIENTS_REGISTERED (org_unit_id, ref_month);
CREATE UNIQUE INDEX IF NOT EXISTS idx_bs_controlled_org_month
    ON heart360tk_reporting.HEART360_BLOOD_SUGAR_CONTROLLED (org_unit_id, ref_month);
CREATE UNIQUE INDEX IF NOT EXISTS idx_bs_severity_org_month
    ON heart360tk_reporting.HEART360_BLOOD_SUGAR_SEVERITY (org_unit_id, ref_month);
CREATE UNIQUE INDEX IF NOT EXISTS idx_bs_missed_visits_org_month
    ON heart360tk_reporting.HEART360_BLOOD_SUGAR_MISSED_VISITS (org_unit_id, ref_month);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dm_bp_control_org_month
    ON heart360tk_reporting.HEART360_DM_BP_CONTROL (org_unit_id, ref_month);
CREATE UNIQUE INDEX IF NOT EXISTS idx_dm_pat_under_care_org_month
    ON heart360tk_reporting.HEART360_DM_PATIENTS_UNDER_CARE (org_unit_id, ref_month);
DROP INDEX IF EXISTS heart360tk_reporting.idx_overdue_patient_id;
CREATE UNIQUE INDEX IF NOT EXISTS idx_overdue_patient_id
    ON heart360tk_reporting.HEART360_OVERDUE_PATIENTS (org_unit_id, patient_id);
CREATE INDEX IF NOT EXISTS idx_overdue_org_last_visit
    ON heart360tk_reporting.HEART360_OVERDUE_PATIENTS (org_unit_id, last_visit_date);
CREATE UNIQUE INDEX IF NOT EXISTS idx_overdue_start_month_org_month
    ON heart360tk_reporting.HEART360_OVERDUE_START_OF_MONTH (org_unit_id, ref_month);
CREATE UNIQUE INDEX IF NOT EXISTS idx_overdue_called_org_month
    ON heart360tk_reporting.HEART360_OVERDUE_PATIENTS_CALLED (org_unit_id, ref_month);
CREATE UNIQUE INDEX IF NOT EXISTS idx_overdue_returned_org_month
    ON heart360tk_reporting.HEART360_OVERDUE_RETURNED_TO_CARE (org_unit_id, ref_month);
CREATE INDEX IF NOT EXISTS idx_cohort_org_quarter
    ON heart360tk_reporting.HEART360_COHORT_PATIENT_DETAILS (org_unit_id, registration_quarter);
CREATE UNIQUE INDEX IF NOT EXISTS idx_pat_dm_cat_org_month
    ON heart360tk_reporting.HEART360_DM_PATIENTS_CATAGORY (org_unit_id, ref_month);

-- ============================================================================
-- STEP 5: Export run audit log (new in 0.5.0).
-- ============================================================================

CREATE TABLE IF NOT EXISTS heart360tk_reporting.export_run_log (
    id               SERIAL          PRIMARY KEY,
    source_key       TEXT            NOT NULL,
    started_at       TIMESTAMPTZ     NOT NULL,
    finished_at      TIMESTAMPTZ,
    status           TEXT            NOT NULL CHECK (status IN ('success', 'failed')),
    duration_seconds NUMERIC(10, 2),
    destination      TEXT,
    error_message    TEXT
);

CREATE INDEX IF NOT EXISTS idx_export_run_log_source_key
    ON heart360tk_reporting.export_run_log (source_key, started_at DESC);

GRANT INSERT, SELECT ON heart360tk_reporting.export_run_log TO heart360tk;
GRANT USAGE ON SEQUENCE heart360tk_reporting.export_run_log_id_seq TO heart360tk;

-- ============================================================================
-- STEP 6: Import run audit log (new in 0.5.0).
-- ============================================================================

CREATE TABLE IF NOT EXISTS heart360tk_reporting.import_run_log (
    id               SERIAL          PRIMARY KEY,
    source_key       TEXT            NOT NULL,
    started_at       TIMESTAMPTZ     NOT NULL,
    finished_at      TIMESTAMPTZ,
    status           TEXT            NOT NULL CHECK (status IN ('success', 'failed')),
    duration_seconds NUMERIC(10, 2),
    error_message    TEXT
);

CREATE INDEX IF NOT EXISTS idx_import_run_log_source_key
    ON heart360tk_reporting.import_run_log (source_key, started_at DESC);

GRANT INSERT, SELECT ON heart360tk_reporting.import_run_log TO heart360tk;
GRANT USAGE ON SEQUENCE heart360tk_reporting.import_run_log_id_seq TO heart360tk;

-- ============================================================================
-- STEP 7: Rename matview_refresh_log -> reporting_table_refresh_log
--         (preserves existing rows), rename its column and index. Guarded so
--         re-running after the rename is a no-op.
-- ============================================================================

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_tables
        WHERE schemaname = 'heart360tk_reporting'
          AND tablename  = 'matview_refresh_log'
    ) AND NOT EXISTS (
        SELECT 1 FROM pg_tables
        WHERE schemaname = 'heart360tk_reporting'
          AND tablename  = 'reporting_table_refresh_log'
    ) THEN
        ALTER TABLE heart360tk_reporting.matview_refresh_log
            RENAME TO reporting_table_refresh_log;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM information_schema.columns
        WHERE table_schema = 'heart360tk_reporting'
          AND table_name   = 'reporting_table_refresh_log'
          AND column_name  = 'matview_name'
    ) THEN
        ALTER TABLE heart360tk_reporting.reporting_table_refresh_log
            RENAME COLUMN matview_name TO reporting_table_name;
    END IF;
END $$;

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'heart360tk_reporting'
          AND indexname  = 'idx_refresh_log_matview_name'
    ) THEN
        ALTER INDEX heart360tk_reporting.idx_refresh_log_matview_name
            RENAME TO idx_refresh_log_table_name;
    END IF;
END $$;

-- Rename the primary-key index/constraint (RENAME TABLE leaves it as the old name).
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'heart360tk_reporting'
          AND indexname  = 'matview_refresh_log_pkey'
    ) THEN
        ALTER INDEX heart360tk_reporting.matview_refresh_log_pkey
            RENAME TO reporting_table_refresh_log_pkey;
    END IF;
END $$;

-- Ensure the log table and all its indexes exist (fresh nodes / partial runs).
CREATE TABLE IF NOT EXISTS heart360tk_reporting.reporting_table_refresh_log (
    id serial PRIMARY KEY,
    reporting_table_name text NOT NULL,
    last_refreshed_at timestamp NOT NULL DEFAULT now(),
    refresh_duration interval,
    status text NOT NULL,
    refresh_batch_id bigint
);

CREATE INDEX IF NOT EXISTS idx_refresh_log_table_name
    ON heart360tk_reporting.reporting_table_refresh_log (reporting_table_name);
CREATE INDEX IF NOT EXISTS idx_refresh_log_last_refreshed
    ON heart360tk_reporting.reporting_table_refresh_log (last_refreshed_at DESC);
CREATE INDEX IF NOT EXISTS idx_refresh_log_batch
    ON heart360tk_reporting.reporting_table_refresh_log (refresh_batch_id);

-- ============================================================================
-- STEP 8: Rename matview_refresh_status -> reporting_table_refresh_status
--         (preserves the singleton status row). Guarded for re-run safety.
-- ============================================================================

DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_tables
        WHERE schemaname = 'heart360tk_reporting'
          AND tablename  = 'matview_refresh_status'
    ) AND NOT EXISTS (
        SELECT 1 FROM pg_tables
        WHERE schemaname = 'heart360tk_reporting'
          AND tablename  = 'reporting_table_refresh_status'
    ) THEN
        ALTER TABLE heart360tk_reporting.matview_refresh_status
            RENAME TO reporting_table_refresh_status;
    END IF;
END $$;

-- Rename the primary-key index/constraint (RENAME TABLE leaves it as the old name).
DO $$
BEGIN
    IF EXISTS (
        SELECT 1 FROM pg_indexes
        WHERE schemaname = 'heart360tk_reporting'
          AND indexname  = 'matview_refresh_status_pkey'
    ) THEN
        ALTER INDEX heart360tk_reporting.matview_refresh_status_pkey
            RENAME TO reporting_table_refresh_status_pkey;
    END IF;
END $$;

-- Ensure the status table exists on fresh nodes / partial runs.
CREATE TABLE IF NOT EXISTS heart360tk_reporting.reporting_table_refresh_status (
    id smallint PRIMARY KEY DEFAULT 1 CHECK (id = 1),
    status text NOT NULL DEFAULT 'idle',
    requested_at timestamptz,
    started_at timestamptz,
    finished_at timestamptz,
    last_error text,
    requested_by text,
    job_name text
);

INSERT INTO heart360tk_reporting.reporting_table_refresh_status (id) VALUES (1)
ON CONFLICT (id) DO NOTHING;

GRANT SELECT ON heart360tk_reporting.reporting_table_refresh_status TO heart360tk;

-- ============================================================================
-- STEP 9: Drop the old matview refresh function.
-- ============================================================================

DROP FUNCTION IF EXISTS heart360tk_reporting.refresh_all_matviews();

-- ============================================================================
-- STEP 10: New table-based refresh function. Truncates each reporting table and
--          repopulates it from the corresponding heart360tk_schema.* view.
-- ============================================================================

CREATE OR REPLACE FUNCTION heart360tk_reporting.refresh_all_reporting_tables()
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_batch_id bigint;
    start_time timestamp;
    end_time timestamp;
    v_table_name text;
    v_rows_affected integer;
    v_tables text[] := ARRAY[
        'heart360_patients_category',
        'heart360_patients_under_care',
        'heart360_patients_registered',
        'heart360_blood_sugar_controlled',
        'heart360_blood_sugar_severity',
        'heart360_blood_sugar_missed_visits',
        'heart360_dm_bp_control',
        'heart360_dm_patients_under_care',
        'heart360_overdue_patients',
        'heart360_overdue_start_of_month',
        'heart360_overdue_patients_called',
        'heart360_overdue_returned_to_care',
        'heart360_cohort_patient_details',
        'heart360_dm_patients_catagory'
    ];
    i integer;
BEGIN
    v_batch_id := EXTRACT(EPOCH FROM clock_timestamp())::bigint;

    FOR i IN 1..array_length(v_tables, 1)
    LOOP
        v_table_name := v_tables[i];

        BEGIN
            start_time := clock_timestamp();
            RAISE NOTICE 'Refreshing %', v_table_name;

            EXECUTE format('TRUNCATE TABLE heart360tk_reporting.%I', v_table_name);
            EXECUTE format('INSERT INTO heart360tk_reporting.%I SELECT * FROM heart360tk_schema.%I', v_table_name, v_table_name);
            GET DIAGNOSTICS v_rows_affected = ROW_COUNT;

            end_time := clock_timestamp();
            INSERT INTO heart360tk_reporting.reporting_table_refresh_log
                (reporting_table_name, last_refreshed_at, refresh_duration, status, refresh_batch_id)
            VALUES (v_table_name, end_time, end_time - start_time, 'success: ' || v_rows_affected || ' rows', v_batch_id);
        EXCEPTION WHEN OTHERS THEN
            INSERT INTO heart360tk_reporting.reporting_table_refresh_log
                (reporting_table_name, last_refreshed_at, refresh_duration, status, refresh_batch_id)
            VALUES (v_table_name, now(), NULL, 'failed: ' || SQLERRM, v_batch_id);
        END;
    END LOOP;

END;
$$;

-- ============================================================================
-- STEP 11: Replace run_refresh_with_status() — now calls
--          refresh_all_reporting_tables(), uses the renamed status table, and
--          short-circuits on central nodes (they receive data via import).
-- ============================================================================

CREATE OR REPLACE FUNCTION heart360tk_reporting.run_refresh_with_status(p_source text DEFAULT 'manual')
RETURNS void
LANGUAGE plpgsql
AS $$
DECLARE
    v_lock_key bigint := hashtext('heart360tk_reporting.matview_refresh');
    v_lock_acquired boolean;
    v_start_time timestamptz;
BEGIN

    IF current_setting('app.is_central_node', true) = 'true' THEN
        RETURN;
    END IF;

    SELECT pg_try_advisory_lock(v_lock_key) INTO v_lock_acquired;
    IF NOT v_lock_acquired THEN
        RAISE NOTICE 'Matview refresh already running (source=%); skipping.', p_source;
        RETURN;
    END IF;

    v_start_time := clock_timestamp();

    BEGIN
        PERFORM heart360tk_reporting.refresh_all_reporting_tables();
        UPDATE heart360tk_reporting.reporting_table_refresh_status
        SET status = 'success',
            started_at = v_start_time,
            finished_at = clock_timestamp(),
            last_error = NULL
        WHERE id = 1;
    EXCEPTION WHEN OTHERS THEN
        UPDATE heart360tk_reporting.reporting_table_refresh_status
        SET status = 'failed',
            started_at = v_start_time,
            finished_at = clock_timestamp(),
            last_error = SQLERRM
        WHERE id = 1;
    END;

    PERFORM pg_advisory_unlock(v_lock_key);
END;
$$;

-- ============================================================================
-- STEP 12: Replace start_async_refresh() — now uses the renamed status table.
-- ============================================================================

CREATE OR REPLACE FUNCTION heart360tk_reporting.start_async_refresh(p_user text DEFAULT NULL)
RETURNS text
LANGUAGE plpgsql
SECURITY DEFINER
AS $$
DECLARE
    v_job_name text;
BEGIN
    UPDATE heart360tk_reporting.reporting_table_refresh_status
    SET status = 'queued',
        requested_at = now(),
        requested_by = p_user,
        last_error = NULL,
        finished_at = NULL
    WHERE id = 1
      AND status NOT IN ('queued', 'in_progress');

    IF NOT FOUND THEN
        RETURN 'already_running';
    END IF;

    v_job_name := 'mv_refresh_oneshot_' || extract(epoch from clock_timestamp())::bigint;

    UPDATE heart360tk_reporting.reporting_table_refresh_status
    SET job_name = v_job_name
    WHERE id = 1;

    PERFORM cron.schedule(
        v_job_name,
        '* * * * *',
        format($cmd$
            DO $body$
            BEGIN
                IF EXISTS (
                    SELECT 1 FROM heart360tk_reporting.reporting_table_refresh_status
                    WHERE id = 1 AND status = 'queued'
                ) THEN
                    UPDATE heart360tk_reporting.reporting_table_refresh_status
                    SET status = 'in_progress', started_at = clock_timestamp()
                    WHERE id = 1;
                    COMMIT;
                    PERFORM heart360tk_reporting.run_refresh_with_status('manual');
                END IF;
                IF EXISTS (SELECT 1 FROM cron.job WHERE jobname = %L) THEN
                    PERFORM cron.unschedule(%L);
                END IF;
            END
            $body$;
        $cmd$, v_job_name, v_job_name)
    );

    RETURN 'queued';
END;
$$;

-- ============================================================================
-- STEP 13: Grants.
-- ============================================================================

GRANT EXECUTE ON FUNCTION heart360tk_reporting.start_async_refresh(text) TO heart360tk;
GRANT EXECUTE ON FUNCTION heart360tk_reporting.run_refresh_with_status(text) TO heart360tk;
GRANT EXECUTE ON FUNCTION heart360tk_reporting.start_async_refresh(text) TO grafana;

-- Cover the new reporting tables for the cached Grafana datasource role.
GRANT SELECT ON ALL TABLES IN SCHEMA heart360tk_reporting TO heart360tk_cached;

-- Re-assert grants on the DM category schema view (Main schema; kept at its
-- original "CATAGORY" spelling — not renamed by this migration).
GRANT SELECT ON heart360tk_schema.HEART360_DM_PATIENTS_CATAGORY TO heart360tk_cached;
GRANT SELECT ON heart360tk_schema.HEART360_DM_PATIENTS_CATAGORY TO heart360tk;

-- ============================================================================
-- STEP 14: Rename the hourly pg_cron job.
--          Old: refresh_matviews_every_hour
--          New: refresh_reporting_tables_every_hour
-- ============================================================================

DO $$
DECLARE
    v_jobid bigint;
BEGIN
    SELECT jobid INTO v_jobid FROM cron.job WHERE jobname = 'refresh_matviews_every_hour';
    IF v_jobid IS NOT NULL THEN
        PERFORM cron.unschedule(v_jobid);
    END IF;
END $$;

SELECT cron.schedule(
    'refresh_reporting_tables_every_hour',
    '0 * * * *',
    'SELECT heart360tk_reporting.run_refresh_with_status(''cron'');'
);

RESET ROLE;

COMMIT;
