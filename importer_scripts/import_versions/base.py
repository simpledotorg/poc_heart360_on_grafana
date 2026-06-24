import logging
import os

from psycopg2 import sql

log = logging.getLogger(__name__)


class BaseImportVersion:
    REPORTING_TABLES: list[str] = []
    SKIP_FILES = frozenset({'metadata.json', 'orgunit.csv'})

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

    def import_org_units(self, conn, extract_dir: str) -> None:
        csv_path = os.path.join(extract_dir, 'orgunit.csv')
        if not os.path.isfile(csv_path):
            log.warning('orgunit.csv not found in zip — skipping org_units import')
            return

        with conn.cursor() as cur:
            cur.execute(
                '''
                CREATE TEMP TABLE tmp_org_units (
                    id        INTEGER,
                    name      VARCHAR(255),
                    level     INTEGER,
                    parent_id INTEGER
                ) ON COMMIT DROP
                '''
            )

        with open(csv_path, 'r', encoding='utf-8') as csv_file:
            with conn.cursor() as cur:
                cur.copy_expert(
                    'COPY tmp_org_units (id, name, level, parent_id) FROM STDIN WITH (FORMAT CSV, HEADER TRUE)',
                    csv_file,
                )

        with conn.cursor() as cur:
            cur.execute(
                '''
                INSERT INTO heart360tk_schema.org_units (id, name, level, parent_id)
                SELECT id, name, level, parent_id FROM tmp_org_units
                ORDER BY level, id
                ON CONFLICT (id) DO UPDATE
                    SET name      = EXCLUDED.name,
                        level     = EXCLUDED.level,
                        parent_id = EXCLUDED.parent_id
                '''
            )
            log.info('  Upserted %d row(s) into heart360tk_schema.org_units', cur.rowcount)

    def copy_csv_to_table(self, conn, table_name: str, csv_path: str) -> None:
        with open(csv_path, 'r', encoding='utf-8') as csv_file:
            with conn.cursor() as cur:
                copy_sql = sql.SQL(
                    'COPY heart360tk_reporting.{} FROM STDIN WITH (FORMAT CSV, HEADER TRUE)'
                ).format(sql.Identifier(table_name))
                cur.copy_expert(copy_sql.as_string(conn), csv_file)
        log.info('  Imported %s from %s', table_name, os.path.basename(csv_path))
