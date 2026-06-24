import os
import sys
import logging
import time
from datetime import datetime, timezone

import psycopg2
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)

IMPORT_CRON = os.getenv('IMPORT_CRON', '0 * * * *').strip()
IMPORT_AGGREAGATE_DATA = os.getenv('IMPORT_AGGREAGATE_DATA', 'false').strip().lower() == 'true'
IMPORT_FOLDER_PATH = os.getenv('IMPORT_FOLDER_PATH', '/export').strip()

DB_CONNECTION_PARAMS = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'database': os.getenv('POSTGRES_DB', 'heart360tk_database'),
    'user': os.getenv('POSTGRES_USER', 'heart360tk'),
    'password': os.getenv('POSTGRES_PASSWORD', ''),
}


def validate_config():
    required = {
        'POSTGRES_PASSWORD': DB_CONNECTION_PARAMS['password'],
    }

    missing = [k for k, v in required.items() if not v]
    if missing:
        log.error('Missing required environment variables: %s', ', '.join(missing))
        sys.exit(1)

    log.info('Config validated OK.')
    log.info('  IMPORT_CRON            : %s', IMPORT_CRON)
    log.info('  IMPORT_AGGREAGATE_DATA : %s', IMPORT_AGGREAGATE_DATA)
    log.info('  IMPORT_FOLDER_PATH     : %s', IMPORT_FOLDER_PATH)


def is_import_enabled():
    if not IMPORT_AGGREAGATE_DATA:
        log.info('IMPORT_AGGREAGATE_DATA=false — import is disabled. Skipping.')
        return False
    return True


def log_import_run(started_at, status, duration_seconds=None, error_message=None):
    try:
        with psycopg2.connect(**DB_CONNECTION_PARAMS) as conn:
            conn.autocommit = True
            with conn.cursor() as cur:
                cur.execute(
                    '''
                    INSERT INTO heart360tk_reporting.import_run_log
                        (source_key, started_at, finished_at, status,
                         duration_seconds, error_message)
                    VALUES (%s, %s, NOW(), %s, %s, %s)
                    ''',
                    (
                        '1',
                        datetime.fromtimestamp(started_at, tz=timezone.utc),
                        status,
                        duration_seconds,
                        error_message,
                    ),
                )
        log.info(
            '  Import run logged to DB — status=%s, duration=%.2fs',
            status,
            duration_seconds or 0,
        )
    except Exception as e:
        log.warning('Could not write import run log to DB (non-fatal): %s', e)


def run_import():
    if not is_import_enabled():
        return

    log.info('=== Import job started ===')
    job_start = time.time()

    try:
        if not os.path.isdir(IMPORT_FOLDER_PATH):
            raise FileNotFoundError(
                f'Import folder does not exist: {IMPORT_FOLDER_PATH}'
            )

        # Placeholder: actual import logic will scan IMPORT_FOLDER_PATH and load data.
        log.info(
            'Import folder ready at %s. No files processed yet.',
            IMPORT_FOLDER_PATH,
        )

        duration = round(time.time() - job_start, 2)
        log_import_run(
            started_at=job_start,
            status='success',
            duration_seconds=duration,
        )
        log.info('=== Import job completed successfully in %.2fs ===', duration)

    except Exception as e:
        duration = round(time.time() - job_start, 2)
        log.error('Import job failed: %s', e, exc_info=True)
        log_import_run(
            started_at=job_start,
            status='failed',
            duration_seconds=duration,
            error_message=str(e),
        )


def start_scheduler():
    try:
        trigger = CronTrigger.from_crontab(IMPORT_CRON)
    except Exception as e:
        log.error("Invalid IMPORT_CRON expression '%s': %s", IMPORT_CRON, e)
        sys.exit(1)

    scheduler = BlockingScheduler()
    scheduler.add_job(run_import, trigger, id='import_job', name='h360tk import')
    log.info("Scheduler started. Import will run on cron: '%s'", IMPORT_CRON)

    try:
        scheduler.start()
    except KeyboardInterrupt:
        log.info('Importer stopped.')
        scheduler.shutdown()


if __name__ == '__main__':
    validate_config()

    if not is_import_enabled():
        log.info('Importer is disabled (IMPORT_AGGREAGATE_DATA=false). Container will exit.')
        sys.exit(0)

    start_scheduler()
