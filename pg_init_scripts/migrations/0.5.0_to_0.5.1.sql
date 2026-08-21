BEGIN;

SET ROLE heart360tk;

SET search_path TO heart360tk_schema;

CREATE OR REPLACE FUNCTION get_descendant_ids(p_parent_id INTEGER)
RETURNS TABLE(id INTEGER)
LANGUAGE sql STABLE
AS $fn$
    WITH RECURSIVE descendants AS (
        SELECT ou.id FROM org_units ou
        WHERE COALESCE(p_parent_id, 0) <> 0 AND ou.id = p_parent_id
        UNION ALL
        SELECT o.id FROM org_units o JOIN descendants d ON o.parent_id = d.id
    )
    SELECT d.id FROM descendants d
    UNION ALL
    SELECT ou.id FROM org_units ou WHERE COALESCE(p_parent_id, 0) = 0;
$fn$;

CREATE OR REPLACE FUNCTION get_user_visible_org_units(p_user_id INTEGER)
RETURNS TABLE(id INTEGER, name VARCHAR, level INTEGER, parent_id INTEGER, is_granted BOOLEAN)
LANGUAGE plpgsql STABLE
AS $fn$
BEGIN
    RETURN QUERY
    WITH allowed AS MATERIALIZED (
        SELECT ou.id, ou.level
        FROM grafana_schema."user" u
        JOIN grafana_schema.team_member tm ON tm.user_id = u.id
        JOIN grafana_schema.team t ON tm.team_id = t.id
        JOIN heart360tk_schema.hierarchy_config hc
          ON t.name ~ ('^heart360tk_' || hc.var_name || '_view_(patients|aggregated)_')
          AND t.name NOT LIKE '%\_ALL' ESCAPE '\'
        JOIN heart360tk_schema.org_units ou
          ON ou.level = hc.level
          AND regexp_replace(lower(trim(ou.name)), '\s+', '_', 'g') =
              regexp_replace(t.name, '^heart360tk_' || hc.var_name || '_view_(patients|aggregated)_', '')
        WHERE u.id = p_user_id
          AND NOT u.is_admin
          AND NOT EXISTS (
              SELECT 1
              FROM grafana_schema.team_member tm2
              JOIN grafana_schema.team t2 ON tm2.team_id = t2.id
              WHERE tm2.user_id = u.id AND t2.name LIKE '%\_ALL' ESCAPE '\'
          )
    ),
 
    reachable AS (
        SELECT lin.ancestor_id AS id           
        FROM heart360tk_schema.org_unit_lineage lin
        JOIN allowed a ON a.id = lin.org_unit_id
        UNION
        SELECT lin.org_unit_id AS id         
        FROM heart360tk_schema.org_unit_lineage lin
        JOIN allowed a ON a.id = lin.ancestor_id
    )
    SELECT x.id, x.name, x.level, x.parent_id,
           EXISTS (SELECT 1 FROM allowed a WHERE a.id = x.id)
    FROM heart360tk_schema.org_units x
    WHERE NOT EXISTS (SELECT 1 FROM allowed)
       OR x.id IN (SELECT r.id FROM reachable r);
END;
$fn$;

GRANT EXECUTE ON FUNCTION heart360tk_schema.get_user_visible_org_units(integer) TO grafana;

-- ============================================================================
-- Watchdog: recovers the admin-dashboard refresh status if it gets stuck
-- (e.g. the database was restarted, or the process running the refresh was
-- killed) while it was 'queued' or 'in_progress', so the status flips to
-- 'failed' and the Refresh button re-enables itself instead of staying
-- stuck forever. Scheduled to run every minute via pg_cron.
-- ============================================================================

CREATE OR REPLACE FUNCTION heart360tk_reporting.reset_stale_refresh_status()
RETURNS void
LANGUAGE plpgsql
AS $fn$
DECLARE
    v_lock_key bigint := hashtext('heart360tk_reporting.matview_refresh');
    v_lock_acquired boolean;
    v_row heart360tk_reporting.reporting_table_refresh_status%ROWTYPE;
    v_stale_job record;
BEGIN
    SELECT * INTO v_row FROM heart360tk_reporting.reporting_table_refresh_status WHERE id = 1;

    IF FOUND AND v_row.status = 'in_progress' THEN
        IF v_row.started_at IS NOT NULL AND v_row.started_at < clock_timestamp() - interval '30 minutes' THEN
            UPDATE heart360tk_reporting.reporting_table_refresh_status
            SET status = 'failed',
                finished_at = clock_timestamp(),
                last_error = 'Refresh timed out after 30 minutes and was automatically reset. Please try again.'
            WHERE id = 1 AND status = 'in_progress';
        ELSIF v_row.started_at IS NOT NULL AND v_row.started_at < clock_timestamp() - interval '30 seconds' THEN
            SELECT pg_try_advisory_lock(v_lock_key) INTO v_lock_acquired;
            IF v_lock_acquired THEN
                PERFORM pg_advisory_unlock(v_lock_key);
                UPDATE heart360tk_reporting.reporting_table_refresh_status
                SET status = 'failed',
                    finished_at = clock_timestamp(),
                    last_error = 'Refresh was interrupted (the cron job was killed, or the database connection/process was lost) and was automatically reset. Please try again.'
                WHERE id = 1 AND status = 'in_progress';
            END IF;
        END IF;
    ELSIF FOUND AND v_row.status = 'queued' THEN
        IF v_row.requested_at IS NOT NULL AND v_row.requested_at < clock_timestamp() - interval '3 minutes' THEN
            UPDATE heart360tk_reporting.reporting_table_refresh_status
            SET status = 'failed',
                finished_at = clock_timestamp(),
                last_error = 'Refresh request was never picked up by the scheduler (it may have restarted) and was automatically reset. Please try again.'
            WHERE id = 1 AND status = 'queued';

            IF v_row.job_name IS NOT NULL AND EXISTS (SELECT 1 FROM cron.job WHERE jobname = v_row.job_name) THEN
                PERFORM cron.unschedule(v_row.job_name);
            END IF;
        END IF;
    END IF;

    -- Defensive sweep: unschedule any one-shot refresh jobs that somehow failed to
    -- unschedule themselves (e.g. the database restarted mid-run) so they don't pile up.
    FOR v_stale_job IN
        SELECT jobname FROM cron.job WHERE jobname LIKE 'mv_refresh_oneshot_%'
    LOOP
        BEGIN
            IF to_timestamp(split_part(v_stale_job.jobname, '_', 4)::bigint) < clock_timestamp() - interval '10 minutes' THEN
                PERFORM cron.unschedule(v_stale_job.jobname);
            END IF;
        EXCEPTION WHEN OTHERS THEN
            NULL; -- unexpected job name format; leave it alone rather than fail the watchdog
        END;
    END LOOP;
END;
$fn$;

SELECT cron.schedule('reset_stale_refresh_status_watchdog', '* * * * *', 'SELECT heart360tk_reporting.reset_stale_refresh_status();');

RESET ROLE;

COMMIT;
