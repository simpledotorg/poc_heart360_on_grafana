import json
import logging
import os
import shutil
import stat
import sys
import tempfile
import time
import zipfile
from datetime import datetime, timezone

import paramiko
import psycopg2
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.triggers.cron import CronTrigger

from import_versions import get_importer
from import_versions.base import BaseImportVersion

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
log = logging.getLogger(__name__)

IMPORT_CRON = os.getenv('IMPORT_CRON', '0 * * * *').strip()
IMPORT_AGGREAGATE_DATA = os.getenv('IMPORT_AGGREAGATE_DATA', 'false').strip().lower() == 'true'
IMPORT_PROTOCOL = os.getenv('IMPORT_PROTOCOL', 'sftp').strip().lower()
IMPORT_FOLDER_PATH = os.getenv('IMPORT_FOLDER_PATH', '/export').strip()

SFTP_HOST = os.getenv('SFTP_HOST', '').strip()
SFTP_PORT = int(os.getenv('SFTP_PORT', '22').strip())
SFTP_USER = os.getenv('SFTP_USER', '').strip()
SFTP_PASSWORD = os.getenv('SFTP_PASSWORD', '').strip()
SFTP_TIMEOUT_SECONDS = int(os.getenv('SFTP_TIMEOUT', '60').strip())

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

    if IMPORT_PROTOCOL == 'sftp':
        required.update({
            'SFTP_HOST': SFTP_HOST,
            'SFTP_USER': SFTP_USER,
            'SFTP_PASSWORD': SFTP_PASSWORD,
        })

    missing = [k for k, v in required.items() if not v]
    if missing:
        log.error('Missing required environment variables: %s', ', '.join(missing))
        sys.exit(1)

    log.info('Config validated OK.')
    log.info('  IMPORT_CRON            : %s', IMPORT_CRON)
    log.info('  IMPORT_AGGREAGATE_DATA : %s', IMPORT_AGGREAGATE_DATA)
    log.info('  IMPORT_PROTOCOL        : %s', IMPORT_PROTOCOL)
    log.info('  IMPORT_FOLDER_PATH     : %s', IMPORT_FOLDER_PATH)
    if IMPORT_PROTOCOL == 'sftp':
        log.info('  SFTP_HOST              : %s', SFTP_HOST)
        log.info('  SFTP_PORT              : %d', SFTP_PORT)
        log.info('  SFTP_USER              : %s', SFTP_USER)
        log.info('  SFTP_TIMEOUT           : %ds', SFTP_TIMEOUT_SECONDS)


def is_import_enabled():
    if not IMPORT_AGGREAGATE_DATA:
        log.info('IMPORT_AGGREAGATE_DATA=false — import is disabled. Skipping.')
        return False
    return True


def log_import_run(
    source_key,
    started_at,
    status,
    duration_seconds=None,
    error_message=None,
):
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
                        source_key,
                        datetime.fromtimestamp(started_at, tz=timezone.utc),
                        status,
                        duration_seconds,
                        error_message,
                    ),
                )
        log.info(
            '  Import run logged to DB — source_key=%s, status=%s, duration=%.2fs',
            source_key,
            status,
            duration_seconds or 0,
        )
    except Exception as e:
        log.warning('Could not write import run log to DB (non-fatal): %s', e)


def _open_sftp_client():
    log.warning(
        'SFTP host key verification is disabled — '
        'set a known_hosts file in production for security.'
    )

    transport = paramiko.Transport((SFTP_HOST, SFTP_PORT))
    transport.banner_timeout = SFTP_TIMEOUT_SECONDS
    transport.auth_timeout = SFTP_TIMEOUT_SECONDS
    transport.connect(username=SFTP_USER, password=SFTP_PASSWORD)

    sftp = paramiko.SFTPClient.from_transport(transport)
    sftp.get_channel().settimeout(SFTP_TIMEOUT_SECONDS)
    return transport, sftp


def _is_import_zip(filename: str) -> bool:
    return (
        filename.lower().endswith('.zip')
        and not filename.startswith('.')
        and not filename.endswith('.tmp')
    )


def fetch_sftp_zip_names() -> list[str]:
    remote_dir = IMPORT_FOLDER_PATH.rstrip('/')
    transport = None
    sftp = None

    try:
        transport, sftp = _open_sftp_client()
        entries = sftp.listdir_attr(remote_dir)
        zip_names = sorted(
            entry.filename
            for entry in entries
            if not stat.S_ISDIR(entry.st_mode) and _is_import_zip(entry.filename)
        )
        log.info(
            'Found %d zip file(s) in %s@%s:%s',
            len(zip_names),
            SFTP_USER,
            SFTP_HOST,
            remote_dir,
        )
        for name in zip_names:
            log.info('  %s', name)
        return zip_names

    finally:
        if sftp is not None:
            try:
                sftp.close()
            except Exception:
                pass
        if transport is not None:
            transport.close()


def download_sftp_zip(zip_name: str, local_zip_path: str) -> None:
    remote_dir = IMPORT_FOLDER_PATH.rstrip('/')
    remote_path = f'{remote_dir}/{zip_name}'
    transport = None
    sftp = None

    try:
        transport, sftp = _open_sftp_client()
        sftp.get(remote_path, local_zip_path)
        log.info('  Downloaded %s', zip_name)
    finally:
        if sftp is not None:
            try:
                sftp.close()
            except Exception:
                pass
        if transport is not None:
            transport.close()


def load_metadata(extract_dir: str) -> dict:
    metadata_path = os.path.join(extract_dir, 'metadata.json')
    if not os.path.isfile(metadata_path):
        raise FileNotFoundError('metadata.json not found in zip')

    with open(metadata_path, 'r', encoding='utf-8') as metadata_file:
        metadata = json.load(metadata_file)

    source_key = metadata.get('source_key', '').strip()
    if not source_key:
        raise ValueError('metadata.json is missing source_key')

    version = metadata.get('import_export_version')
    if version is None:
        raise ValueError('metadata.json is missing import_export_version')

    return metadata


def validate_zip_name(zip_name: str, source_key: str) -> None:
    expected_name = f'{source_key}.zip'
    if zip_name != expected_name:
        log.warning(
            'Zip filename %s does not match expected naming convention %s',
            zip_name,
            expected_name,
        )


def import_zip_file(conn, zip_path: str, zip_name: str) -> str:
    extract_dir = tempfile.mkdtemp(prefix='h360tk_import_')
    try:
        with zipfile.ZipFile(zip_path, 'r') as zf:
            zf.extractall(extract_dir)

        metadata = load_metadata(extract_dir)
        source_key = metadata['source_key']
        version = int(metadata['import_export_version'])

        validate_zip_name(zip_name, source_key)

        importer = get_importer(version)
        importer.import_zip(conn, extract_dir, metadata)
        return source_key

    finally:
        shutil.rmtree(extract_dir, ignore_errors=True)


def run_import():
    if not is_import_enabled():
        return

    log.info('=== Import job started ===')
    job_start = time.time()
    work_dir = tempfile.mkdtemp(prefix='h360tk_import_work_')
    conn = None

    try:
        if IMPORT_PROTOCOL != 'sftp':
            raise ValueError(f'Unsupported IMPORT_PROTOCOL: {IMPORT_PROTOCOL}')

        zip_names = fetch_sftp_zip_names()
        if not zip_names:
            log.info(
                'No zip files found at %s on SFTP server — nothing to import.',
                IMPORT_FOLDER_PATH,
            )
            return

        conn = psycopg2.connect(**DB_CONNECTION_PARAMS)
        conn.autocommit = False

        log.info('Truncating reporting tables before import...')
        BaseImportVersion.truncate_reporting_tables(conn)
        conn.commit()

        imported_count = 0
        for zip_name in zip_names:
            zip_start = time.time()
            local_zip_path = os.path.join(work_dir, zip_name)
            source_key = os.path.splitext(zip_name)[0]

            try:
                download_sftp_zip(zip_name, local_zip_path)
                source_key = import_zip_file(conn, local_zip_path, zip_name)
                conn.commit()

                duration = round(time.time() - zip_start, 2)
                log_import_run(
                    source_key=source_key,
                    started_at=zip_start,
                    status='success',
                    duration_seconds=duration,
                )
                imported_count += 1
                log.info(
                    '=== Imported zip %s (source_key=%s) in %.2fs ===',
                    zip_name,
                    source_key,
                    duration,
                )

            except Exception as e:
                conn.rollback()
                duration = round(time.time() - zip_start, 2)
                log.error(
                    'Failed to import zip %s (source_key=%s): %s',
                    zip_name,
                    source_key,
                    e,
                    exc_info=True,
                )
                log_import_run(
                    source_key=source_key,
                    started_at=zip_start,
                    status='failed',
                    duration_seconds=duration,
                    error_message=str(e),
                )

        if imported_count == 0:
            log.error('No zip files were imported successfully')
        else:
            duration = round(time.time() - job_start, 2)
            log.info(
                '=== Import job completed — %d/%d zip(s) imported in %.2fs ===',
                imported_count,
                len(zip_names),
                duration,
            )

    except Exception as e:
        if conn is not None:
            conn.rollback()
        duration = round(time.time() - job_start, 2)
        log.error('Import job failed: %s', e, exc_info=True)

    finally:
        if conn is not None:
            conn.close()
        shutil.rmtree(work_dir, ignore_errors=True)


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
