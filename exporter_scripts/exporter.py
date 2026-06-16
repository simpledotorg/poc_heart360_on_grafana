import os
import sys
import csv
import json
import logging
import tempfile
import shutil
import time
import zipfile
from datetime import datetime, timezone
import psycopg2
from psycopg2 import sql
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
log = logging.getLogger(__name__)

NODE_ROLE        = os.getenv('NODE_ROLE', 'leaf').strip().lower()
SOURCE_KEY       = os.getenv('SOURCE_KEY', '').strip()
SOURCE_VERSION   = os.getenv('SOURCE_VERSION', '').strip()
PROTOCOL_VERSION = int(os.getenv('PROTOCOL_VERSION', '1').strip())
EXPORT_CRON      = os.getenv('EXPORT_CRON', '0 * * * *').strip()  # default: every hour
UPLOAD_DEST_PATH = os.getenv('UPLOAD_DEST_PATH', '/export').strip()

DEFAULT_EXPORT_TABLES = [
    'heart360_patients_category',
    'heart360_patients_under_care',
    'heart360_patients_registered',
    'heart360_blood_sugar_controlled',
    'heart360_blood_sugar_severity',
    'heart360_blood_sugar_missed_visits',
    'heart360_dm_bp_control',
    'heart360_dm_patients_under_care',
    'heart360_overdue_start_of_month',
    'heart360_overdue_patients_called',
    'heart360_overdue_returned_to_care',
    'heart360_cohort_patient_details',
]

_export_tables_env = os.getenv('EXPORT_TABLES', '').strip()
EXPORT_TABLES = (
    [t.strip() for t in _export_tables_env.split(',') if t.strip()]
    if _export_tables_env
    else DEFAULT_EXPORT_TABLES
)

DB_CONNECTION_PARAMS = {
    'host':     os.getenv('POSTGRES_HOST', 'postgres'),
    'database': os.getenv('POSTGRES_DB', 'heart360tk_database'),
    'user':     os.getenv('POSTGRES_USER', 'heart360tk'),
    'password': os.getenv('POSTGRES_PASSWORD', ''),
}

def validate_config():
    required = {
        'SOURCE_KEY':        SOURCE_KEY,
        'SOURCE_VERSION':    SOURCE_VERSION,
        'POSTGRES_PASSWORD': DB_CONNECTION_PARAMS['password'],
    }
    missing = [k for k, v in required.items() if not v]
    if missing:
        log.error("Missing required environment variables: %s", ', '.join(missing))
        sys.exit(1)

    log.info("Config validated OK.")
    log.info("  NODE_ROLE        : %s", NODE_ROLE)
    log.info("  SOURCE_KEY       : %s", SOURCE_KEY)
    log.info("  SOURCE_VERSION   : %s", SOURCE_VERSION)
    log.info("  PROTOCOL_VERSION : %d", PROTOCOL_VERSION)
    log.info("  EXPORT_CRON      : %s", EXPORT_CRON)
    log.info("  UPLOAD_DEST_PATH : %s", UPLOAD_DEST_PATH)
    log.info("  EXPORT_TABLES    : %s", EXPORT_TABLES)

def is_export_enabled():
    if NODE_ROLE == 'central':
        log.info("NODE_ROLE=central — export is disabled on this node. Skipping.")
        return False
    return True


def generate_csvs(conn):
    tmp_dir = tempfile.mkdtemp(prefix='h360tk_export_')
    log.info("Writing CSVs to temp dir: %s", tmp_dir)

    stats = {
        'facility_count': 0,
        'patient_count':  0,
    }

    cur = conn.cursor()
    try:
        # --- Write one CSV per aggregate table ---
        for table_name in EXPORT_TABLES:
            csv_path = os.path.join(tmp_dir, f'{table_name.upper()}.csv')
            try:
                # Use psycopg2.sql for safe identifier quoting — prevents SQL injection
                cur.execute(
                    sql.SQL('SELECT * FROM heart360tk_reporting.{}').format(
                        sql.Identifier(table_name)
                    )
                )
                rows = cur.fetchall()
                col_names = [desc[0] for desc in cur.description]

                with open(csv_path, 'w', newline='', encoding='utf-8') as f:
                    writer = csv.writer(f)
                    writer.writerow(col_names)
                    writer.writerows(rows)

                log.info("  Written %s — %d rows", f'{table_name.upper()}.csv', len(rows))

            except Exception as e:
                log.error("  Failed to export table '%s': %s", table_name, e)
                raise

        # --- Write orgunit.csv (full hierarchy — all levels with parent_id) ---
        orgunit_path = os.path.join(tmp_dir, 'orgunit.csv')
        cur.execute(
            'SELECT id, name, level, parent_id FROM heart360tk_schema.org_units ORDER BY level, id'
        )
        rows = cur.fetchall()
        col_names = [desc[0] for desc in cur.description]

        with open(orgunit_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(col_names)
            writer.writerows(rows)

        # Count only facility-level org units (level=3) for metadata check
        facility_level = 3
        try:
            cur.execute(
                "SELECT level FROM heart360tk_schema.hierarchy_config WHERE var_name = 'facility'"
            )
            row = cur.fetchone()
            if row:
                facility_level = row[0]
        except Exception as e:
            log.warning("Could not fetch facility level, defaulting to 3: %s", e)

        stats['facility_count'] = sum(1 for r in rows if r[2] == facility_level)
        log.info("  Written orgunit.csv — %d org units (facility count: %d)",
                 len(rows), stats['facility_count'])

        try:
            cur.execute(
                '''
                SELECT COUNT(DISTINCT p.patient_id)
                FROM heart360tk_schema.patients p
                WHERE LOWER(p.patient_status) <> 'dead'
                  AND EXISTS (
                      SELECT 1 FROM heart360tk_schema.patient_diagnoses pd
                      WHERE pd.patient_id = p.patient_id
                        AND pd.diagnosis_code IN ('I10', 'E11')
                  )
                '''
            )
            result = cur.fetchone()
            stats['patient_count'] = int(result[0]) if result and result[0] else 0
        except Exception as e:
            log.warning("Could not fetch patient count: %s", e)
            stats['patient_count'] = 0

    except Exception:
        shutil.rmtree(tmp_dir, ignore_errors=True)
        raise

    finally:
        cur.close()

    log.info("CSV generation complete. Facilities: %d, Patients: %d",
             stats['facility_count'], stats['patient_count'])

    return tmp_dir, stats


def generate_metadata(tmp_dir, stats, generation_start_epoch):
    now_epoch = time.time()
    now_human = datetime.fromtimestamp(now_epoch, tz=timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    generation_duration = round(now_epoch - generation_start_epoch, 2)

    metadata = {
        'protocol_version':            PROTOCOL_VERSION,
        'source_key':                  SOURCE_KEY,
        'source_h360tk_version':       SOURCE_VERSION,
        'generated_at':          now_human,
        'generated_at_epoch':          int(now_epoch),
        'generation_duration_seconds': generation_duration,
        'facility_count':              stats['facility_count'],
        'patient_count':               stats['patient_count'],
    }

    metadata_path = os.path.join(tmp_dir, 'metadata.json')
    with open(metadata_path, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2)

    log.info("  Written metadata.json — source_key=%s, version=%s, facilities=%d, patients=%d",
             SOURCE_KEY, SOURCE_VERSION,
             stats['facility_count'], stats['patient_count'])

    return metadata_path


def package_zip(tmp_dir):
    epoch = int(time.time())
    zip_filename = f'{SOURCE_KEY}_{epoch}.zip'
    zip_path = os.path.join('/tmp', zip_filename)
    zip_tmp_path = zip_path + '.tmp'

    try:
        with zipfile.ZipFile(zip_tmp_path, 'w', compression=zipfile.ZIP_DEFLATED) as zf:
            for filename in sorted(os.listdir(tmp_dir)):
                file_path = os.path.join(tmp_dir, filename)
                if os.path.isfile(file_path):
                    zf.write(file_path, arcname=filename)
                    log.info("  Added to zip: %s", filename)

        # Atomic rename — only appears at final path once fully written
        os.rename(zip_tmp_path, zip_path)

    except Exception:
        # Remove partial zip if something went wrong
        if os.path.exists(zip_tmp_path):
            os.remove(zip_tmp_path)
        raise

    zip_size_kb = round(os.path.getsize(zip_path) / 1024, 2)
    log.info("  Zip created: %s (%.2f KB)", zip_filename, zip_size_kb)

    shutil.rmtree(tmp_dir, ignore_errors=True)
    log.info("  Temp dir cleaned up.")

    return zip_path


def upload_zip(zip_path):
    os.makedirs(UPLOAD_DEST_PATH, exist_ok=True)
    dest = os.path.join(UPLOAD_DEST_PATH, os.path.basename(zip_path))

    if os.path.abspath(zip_path) != os.path.abspath(dest):
        shutil.copy2(zip_path, dest)
        os.remove(zip_path)

    log.info("  Zip uploaded to: %s", dest)
    return dest


def run_export():
    if not is_export_enabled():
        return

    log.info("=== Export job started (source_key=%s) ===", SOURCE_KEY)
    job_start = time.time()

    conn = None
    try:
        conn = psycopg2.connect(**DB_CONNECTION_PARAMS)
        conn.autocommit = True

        tmp_dir, stats = generate_csvs(conn)
        generate_metadata(tmp_dir, stats, generation_start_epoch=job_start)
        zip_path = package_zip(tmp_dir)
        destination = upload_zip(zip_path)
        log.info("=== Export job completed successfully. Destination: %s ===", destination)

    except psycopg2.Error as e:
        log.error("Database connection error: %s", e)

    except Exception as e:
        log.error("Export job failed: %s", e, exc_info=True)

    finally:
        if conn:
            conn.close()



def start_scheduler():
    try:
        trigger = CronTrigger.from_crontab(EXPORT_CRON)
    except Exception as e:
        log.error("Invalid EXPORT_CRON expression '%s': %s", EXPORT_CRON, e)
        sys.exit(1)

    scheduler = BlockingScheduler()
    scheduler.add_job(run_export, trigger, id='export_job', name='h360tk export')
    log.info("Scheduler started. Export will run on cron: '%s'", EXPORT_CRON)

    try:
        scheduler.start()
    except KeyboardInterrupt:
        log.info("Exporter stopped.")
        scheduler.shutdown()


if __name__ == '__main__':
    validate_config()

    if not is_export_enabled():
        log.info("Exporter is disabled (NODE_ROLE=central). Container will exit.")
        sys.exit(0)

    start_scheduler()
