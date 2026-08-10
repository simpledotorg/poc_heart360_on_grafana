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

RESET ROLE;

COMMIT;
