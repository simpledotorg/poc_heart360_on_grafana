import csv
import logging
import os
from datetime import datetime, timezone

from psycopg2 import sql

log = logging.getLogger(__name__)


class BaseImportVersion:
    REPORTING_TABLES: list[str] = []
    ORG_UNITS_TABLE: str = 'org_units'
    SKIP_FILES = frozenset({'metadata.json'})

    @classmethod
    def truncate_reporting_tables(cls, conn) -> None:
        from import_versions import all_reporting_tables

        with conn.cursor() as cur:
            for table_name in all_reporting_tables():
                cur.execute(
                    sql.SQL('TRUNCATE TABLE heart360tk_reporting.{}').format(
                        sql.Identifier(table_name)
                    )
                )
                log.info('  Truncated heart360tk_reporting.%s', table_name)

    def import_zip(self, conn, extract_dir: str, metadata: dict) -> None:
        raise NotImplementedError

    def csv_to_table_name(self, csv_filename: str) -> str | None:
        table_name = os.path.splitext(csv_filename)[0].lower()
        if table_name in self.REPORTING_TABLES:
            return table_name
        return None

    def _read_orgunit_rows(self, csv_path: str) -> list[dict]:
        rows = []
        with open(csv_path, 'r', encoding='utf-8') as csv_file:
            reader = csv.DictReader(csv_file)
            for row in reader:
                parent_raw = (row.get('parent_id') or '').strip()
                rows.append({
                    'leaf_id': int(row['id']),
                    'name': row['name'].strip(),
                    'level': int(row['level']),
                    'leaf_parent_id': int(parent_raw) if parent_raw else None,
                })
        rows.sort(key=lambda item: (item['level'], item['leaf_id']))
        return rows

    def _upsert_org_unit_mapping(
        self,
        conn,
        source_key: str,
        leaf_org_unit_id: int,
        central_org_unit_id: int,
        metadata: dict,
    ) -> None:
        """Persist leaf→central org_unit id mapping (all hierarchy levels)."""
        extract_epoch = metadata.get('generated_at_epoch')
        last_extract_date = (
            datetime.fromtimestamp(extract_epoch, tz=timezone.utc)
            if extract_epoch is not None
            else None
        )

        with conn.cursor() as cur:
            cur.execute(
                '''
                INSERT INTO heart360tk_reporting.import_facility_mapping
                    (leaf_node_key, leaf_node_facility_id, central_node_facility_id,
                     last_updated_date, last_extract_date)
                VALUES (%s, %s, %s, NOW(), %s)
                ''',
                (source_key, leaf_org_unit_id, central_org_unit_id, last_extract_date),
            )

    def import_org_units(
        self,
        conn,
        extract_dir: str,
        source_key: str,
        metadata: dict,
    ) -> dict[int, int]:
        """Merge org unit CSV into central org_units; return leaf_id -> central_id map.

        Looks for ORG_UNITS.csv (new exporter format).  Falls back to orgunit.csv
        when the zip was created by an older exporter; in that case org_units and
        hierarchy_config are also copied directly from heart360tk_schema into the
        reporting tables (since the new-format CSVs are absent from the zip).

        Each leaf node may define its own hierarchy depth (e.g. 2, 4, or 5 levels).
        Rows are processed parent-before-child using level order from the CSV.
        Every leaf org_unit id is recorded in import_facility_mapping for reporting
        table id remapping, regardless of level.

        Note: for new-format zips, the reporting table is populated (with remapped ids)
        by copy_csv_to_table() in the CSV loop, exactly like every other reporting table.
        """
        csv_filename = f'{self.ORG_UNITS_TABLE.upper()}.csv'
        csv_path = os.path.join(extract_dir, csv_filename)

        # Backward compat: zips created by the old exporter used 'orgunit.csv'.
        # When only the legacy file exists, we still build the id mapping and
        # then copy org_units / hierarchy_config from heart360tk_schema directly
        # into the reporting tables (the new-format CSV loop won't find them).
        legacy_format = False
        if not os.path.isfile(csv_path):
            legacy_path = os.path.join(extract_dir, 'orgunit.csv')
            if os.path.isfile(legacy_path):
                log.warning(
                    '%s not found; falling back to legacy orgunit.csv '
                    '(zip was created by an older exporter)',
                    csv_filename,
                )
                csv_path = legacy_path
                legacy_format = True
            else:
                raise FileNotFoundError(f'{csv_filename} not found in zip')

        org_rows = self._read_orgunit_rows(csv_path)
        leaf_to_central: dict[int, int] = {}

        with conn.cursor() as cur:
            cur.execute(
                '''
                DELETE FROM heart360tk_reporting.import_facility_mapping
                WHERE leaf_node_key = %s
                ''',
                (source_key,),
            )

            for row in org_rows:
                leaf_id = row['leaf_id']
                name = row['name']
                level = row['level']
                leaf_parent_id = row['leaf_parent_id']

                central_parent_id = None
                if leaf_parent_id is not None:
                    central_parent_id = leaf_to_central.get(leaf_parent_id)
                    if central_parent_id is None:
                        raise ValueError(
                            f'{csv_filename} parent id {leaf_parent_id} for leaf id {leaf_id} '
                            f'was not processed before its child (source_key={source_key})'
                        )

                cur.execute(
                    'SELECT heart360tk_schema.upsert_org_unit(%s, %s, %s)',
                    (name, level, central_parent_id),
                )
                central_id = cur.fetchone()[0]
                if central_id is None:
                    raise ValueError(
                        f'Could not upsert org unit {name!r} (level={level}, '
                        f'parent_id={central_parent_id}) for source_key={source_key}'
                    )

                leaf_to_central[leaf_id] = central_id
                self._upsert_org_unit_mapping(
                    conn, source_key, leaf_id, central_id, metadata
                )
                log.info(
                    '  Mapped org unit leaf_id=%d -> central_id=%d (%s, level=%d)',
                    leaf_id,
                    central_id,
                    name,
                    level,
                )

        log.info(
            '  Merged %d org unit(s) for source_key=%s',
            len(leaf_to_central),
            source_key,
        )

        if legacy_format:
            # Old zip: ORG_UNITS.csv / HIERARCHY_CONFIG.csv are absent so the
            # CSV loop won't populate the reporting tables.  Copy them directly
            # from heart360tk_schema (which was just updated by upsert_org_unit
            # above) so Grafana sees data without requiring a re-export.
            with conn.cursor() as cur:
                cur.execute(
                    '''
                    INSERT INTO heart360tk_reporting.org_units
                        (id, name, level, parent_id)
                    SELECT id, name, level, parent_id
                    FROM heart360tk_schema.org_units
                    ON CONFLICT (id) DO NOTHING
                    '''
                )
                cur.execute(
                    '''
                    INSERT INTO heart360tk_reporting.hierarchy_config
                        (level, display_name, var_name)
                    SELECT level, display_name, var_name
                    FROM heart360tk_schema.hierarchy_config
                    ON CONFLICT (level) DO NOTHING
                    '''
                )
            log.info('  Copied org_units and hierarchy_config from schema (legacy fallback)')

        return leaf_to_central

    def _get_table_columns(self, conn, table_name: str) -> list[str]:
        with conn.cursor() as cur:
            cur.execute(
                '''
                SELECT column_name
                FROM information_schema.columns
                WHERE table_schema = 'heart360tk_reporting'
                  AND table_name = %s
                ORDER BY ordinal_position
                ''',
                (table_name,),
            )
            return [row[0] for row in cur.fetchall()]

    def copy_csv_to_table(
        self,
        conn,
        table_name: str,
        csv_path: str,
        source_key: str,
    ) -> None:
        table_columns = self._get_table_columns(conn, table_name)
        if not table_columns:
            raise ValueError(f'No columns found for reporting table {table_name}')

        temp_table = f'tmp_import_{table_name}'

        with conn.cursor() as cur:
            cur.execute(
                sql.SQL(
                    'CREATE TEMP TABLE {} (LIKE heart360tk_reporting.{} INCLUDING DEFAULTS) '
                    'ON COMMIT DROP'
                ).format(
                    sql.Identifier(temp_table),
                    sql.Identifier(table_name),
                )
            )

            with open(csv_path, 'r', encoding='utf-8') as csv_file:
                cur.copy_expert(
                    sql.SQL(
                        'COPY {} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)'
                    ).format(sql.Identifier(temp_table)).as_string(conn),
                    csv_file,
                )

            if table_name == 'org_units':
                # org_units carries its own leaf-node primary key (id) and a
                # self-referencing parent_id — both must be remapped to central
                # IDs via import_facility_mapping, just like org_unit_id is
                # remapped in the HEART360_* reporting tables.
                #
                # Reporting tables are truncated once per import run and then every
                # leaf node's CSV is appended, so nodes that share an ancestor
                # (same central id) would collide on the UNIQUE(id) index — ON
                # CONFLICT DO NOTHING keeps the first (identical) row.
                insert_sql = sql.SQL(
                    'INSERT INTO heart360tk_reporting.org_units (id, name, level, parent_id) '
                    'SELECT '
                    '    m.central_node_facility_id, '
                    '    t.name, '
                    '    t.level, '
                    '    mp.central_node_facility_id '
                    'FROM {temp} t '
                    'JOIN heart360tk_reporting.import_facility_mapping m '
                    '  ON m.leaf_node_key = %s AND m.leaf_node_facility_id = t.id '
                    'LEFT JOIN heart360tk_reporting.import_facility_mapping mp '
                    '  ON mp.leaf_node_key = %s AND mp.leaf_node_facility_id = t.parent_id '
                    'ON CONFLICT (id) DO NOTHING'
                ).format(temp=sql.Identifier(temp_table))
                cur.execute(insert_sql, (source_key, source_key))
            elif table_name == 'hierarchy_config':
                # Global dimension identical across every leaf node; no id remap.
                # The first node populates all levels; later nodes are no-ops via
                # ON CONFLICT on the UNIQUE(level) index (see truncate-once note above).
                cur.execute(
                    sql.SQL(
                        'INSERT INTO heart360tk_reporting.hierarchy_config '
                        'SELECT * FROM {temp} ON CONFLICT (level) DO NOTHING'
                    ).format(temp=sql.Identifier(temp_table))
                )
            elif 'org_unit_id' in table_columns:
                select_parts = [
                    sql.SQL('m.central_node_facility_id') if col == 'org_unit_id'
                    else sql.SQL('t.{}').format(sql.Identifier(col))
                    for col in table_columns
                ]
                insert_sql = sql.SQL(
                    'INSERT INTO heart360tk_reporting.{target} ({columns}) '
                    'SELECT {select_exprs} '
                    'FROM {temp} t '
                    'JOIN heart360tk_reporting.import_facility_mapping m '
                    '  ON m.leaf_node_key = %s '
                    ' AND m.leaf_node_facility_id = t.org_unit_id'
                ).format(
                    target=sql.Identifier(table_name),
                    columns=sql.SQL(', ').join(map(sql.Identifier, table_columns)),
                    select_exprs=sql.SQL(', ').join(select_parts),
                    temp=sql.Identifier(temp_table),
                )
                cur.execute(insert_sql, (source_key,))
            else:
                cur.execute(
                    sql.SQL(
                        'INSERT INTO heart360tk_reporting.{target} '
                        'SELECT * FROM {temp}'
                    ).format(
                        target=sql.Identifier(table_name),
                        temp=sql.Identifier(temp_table),
                    )
                )

            inserted = cur.rowcount

        if inserted == 0:
            log.warning(
                '  No rows imported into %s from %s',
                table_name,
                os.path.basename(csv_path),
            )
        else:
            log.info(
                '  Imported %d row(s) into %s from %s',
                inserted,
                table_name,
                os.path.basename(csv_path),
            )
