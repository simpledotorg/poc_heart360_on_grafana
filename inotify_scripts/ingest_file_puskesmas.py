import sys
import json
import pandas as pd
from datetime import datetime
import re
import os
import psycopg2

# --- CONFIGURATION (EXPECTED COLUMNS) ---
EXPECTED_COLUMNS = [
    'puskesmas',
    'district',
    'shc',
    'nik',
    'no_rm_lama',
    'nama_pasien',
    'tgl_lahir',
    'jenis_kelamin',
    'no_telp',
    'tanggal_pendaftaran',
    'kunjungan_terakhir',
    'sistole',
    'diastole',
    'tgl_terjadwal',
    'tgl_panggilan',
    'jenis_hasil',
    'alasan_dihapus',
    'gula_darah',
    'jenis_gula_darah',
    'wilayah',
    # Optional followup date columns (used when present)
    'tgl_kunjungan_ht',       # HTN_LastFollowup_Completed_Date
    'tgl_kunjungan_dm',       # Diabetes_LastFollowup_Completed_Date
]
# ---------------------------------

DATE_FORMAT_IN = "%d-%m-%Y"
DATE_FORMAT_OUT = "%Y-%m-%d"

# --- DATABASE CONNECTION DETAILS ---
DB_CONNECTION_PARAMS = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'database': os.getenv('POSTGRES_DB', 'metrics_db'),
    'user': os.getenv('POSTGRES_USER', 'grafana_user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'your_db_password'),
}
SP_REGION_VALUE = 'Global'

# --- HIERARCHY CONFIGURATION ---
# To add a new hierarchy level, just add a row here.
# The ingestion script auto-syncs this to the DB hierarchy_config table,
# and pre-created Grafana variables (level_6..level_10) auto-populate.
#
# Fields:
#   level        – integer depth (1 = top)
#                  Example: 1, 2, 3, ... 6
#   column       – Excel column name(s) to read (first match wins)
#                  Example: ['wilayah'], ['district'], ['small_village']
#   display_name – label for readability only. Levels 1–5 have fixed names in
#                  Grafana (Region, District, Facility, Sub-Facility, Village); only levels 6+
#                  can be customized via this field but it is not display in grafana.
#   var_name     – Levels 1–5 use fixed names (region, district, facility, sub_facility, village);
#                  only levels 6+ need this (e.g. level_6, level_7).
#   default      – fallback value when column is empty (None = skip level)

HIERARCHY_LEVELS = [
    {'level': 1, 'column': ['wilayah'],          'display_name': 'Region',   'var_name': 'region',   'default': SP_REGION_VALUE},
    {'level': 2, 'column': ['district'],          'display_name': 'District', 'var_name': 'district', 'default': None},
    {'level': 3, 'column': ['puskesmas'],         'display_name': 'Facility',      'var_name': 'facility',      'default': 'UNKNOWN'},
    {'level': 4, 'column': ['shc'],               'display_name': 'Sub-Facility',  'var_name': 'sub_facility',  'default': None},
    {'level': 5, 'column': ['village'],            'display_name': 'Village',  'var_name': 'village',  'default': None},
    {'level': 6, 'column': ['small_village'],       'display_name': 'Level 6',   'var_name': 'level_6',  'default': None},
]
# ----------------------------------------------------------------

def clean_blood_pressure(value):
    if pd.isna(value) or value is None:
        return None
    s = str(value).strip()
    cleaned_s = re.sub(r'[^\d.]', '', s)
    try:
        if not cleaned_s or cleaned_s == '.':
            return None
        return float(cleaned_s)
    except ValueError:
        return None

def parse_date_field(value):
    if pd.isna(value) or value is None:
        return None
    if isinstance(value, (datetime, pd.Timestamp)):
        return value
    if isinstance(value, str):
        value = value.strip()
        if not value or value.lower() == 'nan':
            return None
        try:
            return datetime.strptime(value, DATE_FORMAT_IN)
        except ValueError:
            pass
        try:
            parsed = pd.to_datetime(value)
            return parsed.to_pydatetime() if hasattr(parsed, 'to_pydatetime') else parsed
        except (ValueError, TypeError):
            return None
    try:
        parsed = pd.to_datetime(value)
        return parsed.to_pydatetime() if hasattr(parsed, 'to_pydatetime') else parsed
    except (ValueError, TypeError):
        return None

def safe_str(value):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        return None
    return str(value)

def to_sql_literal(value, target_type=None):
    if value is None or (isinstance(value, float) and pd.isna(value)):
        if target_type == 'bigint':
            return 'NULL::BIGINT'
        elif target_type == 'DATE':
            return 'NULL::DATE'
        elif target_type == 'TIMESTAMP':
            return 'NULL::TIMESTAMP'
        elif target_type == 'NUMERIC':
            return 'NULL::NUMERIC'
        else:
            return 'NULL::VARCHAR'

    if target_type == 'bigint':
        val_str = str(value).strip()
        if val_str.endswith('.0'):
            val_str = val_str[:-2]
        if not val_str.isdigit():
            return 'NULL::BIGINT'
        return f"CAST('{val_str}' AS bigint)"

    if target_type == 'DATE' and isinstance(value, (datetime, pd.Timestamp)):
        return f"'{value.strftime('%Y-%m-%d')}'::DATE"

    if target_type == 'TIMESTAMP' and isinstance(value, (datetime, pd.Timestamp)):
        return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'::timestamp"

    if isinstance(value, str):
        return f"'{value.replace(chr(39), chr(39)+chr(39))}'::VARCHAR"

    if isinstance(value, (int, float)):
        return str(value)

    return f"'{str(value).replace(chr(39), chr(39)+chr(39))}'::VARCHAR"

def execute_upsert_org_unit_chain(cur, hierarchy):
    """Upsert org_unit hierarchy chain and return leaf org_unit_id.
    hierarchy: list of (name, level) tuples from top to bottom.
    Skips entries with None names.
    """
    names = [h[0] for h in hierarchy if h[0] is not None]
    levels = [h[1] for h in hierarchy if h[0] is not None]
    if not names:
        return None
    names_literal = "ARRAY[" + ",".join(to_sql_literal(n) for n in names) + "]"
    levels_literal = "ARRAY[" + ",".join(str(l) for l in levels) + "]"
    sql = f"SELECT upsert_org_unit_chain({names_literal}::VARCHAR[], {levels_literal}::INTEGER[]);"
    cur.execute(sql)
    return cur.fetchone()[0]

def execute_upsert_patient(cur, patient_id_sql, record, registration_date_parsed, birth_date_parsed, org_unit_id):
    """Insert new patient or update registration_date if earlier."""
    sql = f"""
INSERT INTO patients (patient_id, patient_name, gender, phone_number, patient_status, registration_date, birth_date, org_unit_id)
VALUES (
    {patient_id_sql},
    {to_sql_literal(safe_str(record.get('nama_pasien')))},
    {to_sql_literal(safe_str(record.get('jenis_kelamin')))},
    {to_sql_literal(safe_str(record.get('no_telp')))},
    'ALIVE'::VARCHAR,
    {to_sql_literal(registration_date_parsed, target_type='TIMESTAMP')},
    {to_sql_literal(birth_date_parsed, target_type='DATE')},
    {org_unit_id}
)
ON CONFLICT (patient_id) DO UPDATE SET
    registration_date = LEAST(patients.registration_date, EXCLUDED.registration_date);
"""
    cur.execute(sql)

def execute_insert_encounter(cur, patient_id_sql, encounter_datetime_parsed, org_unit_id):
    """Create encounter (or get existing). Returns encounter_id."""
    sql = f"""
INSERT INTO encounters (patient_id, encounter_date, org_unit_id)
VALUES ({patient_id_sql}, {to_sql_literal(encounter_datetime_parsed, target_type='TIMESTAMP')}, {org_unit_id})
ON CONFLICT (patient_id, encounter_date)
DO UPDATE SET org_unit_id = EXCLUDED.org_unit_id
RETURNING id;
"""
    cur.execute(sql)
    return cur.fetchone()[0]

def encounter_exists(cur, patient_id_sql, encounter_date):
    """Check if an encounter already exists for this patient on this date."""
    sql = f"""
SELECT 1 FROM encounters
WHERE patient_id = {patient_id_sql}
  AND encounter_date = {to_sql_literal(encounter_date, target_type='TIMESTAMP')}
LIMIT 1;
"""
    cur.execute(sql)
    return cur.fetchone() is not None

def execute_insert_bp(cur, encounter_id, systolic, diastolic):
    """Insert blood pressure for an encounter."""
    if systolic is None and diastolic is None:
        return
    sql = f"""
INSERT INTO blood_pressures (encounter_id, systolic_bp, diastolic_bp)
VALUES ({encounter_id}, {to_sql_literal(systolic, target_type='NUMERIC')}, {to_sql_literal(diastolic, target_type='NUMERIC')})
ON CONFLICT (encounter_id) DO UPDATE SET
    systolic_bp = EXCLUDED.systolic_bp, diastolic_bp = EXCLUDED.diastolic_bp;
"""
    cur.execute(sql)

def execute_insert_bs(cur, encounter_id, blood_sugar_type, blood_sugar_value):
    """Insert blood sugar for an encounter."""
    if blood_sugar_value is None:
        return
    sql = f"""
INSERT INTO blood_sugars (encounter_id, blood_sugar_type, blood_sugar_value)
VALUES ({encounter_id}, {to_sql_literal(safe_str(blood_sugar_type))}, {to_sql_literal(blood_sugar_value, target_type='NUMERIC')})
ON CONFLICT (encounter_id) DO UPDATE SET
    blood_sugar_type = EXCLUDED.blood_sugar_type, blood_sugar_value = EXCLUDED.blood_sugar_value;
"""
    cur.execute(sql)

# --- MAIN INGESTION AND EXECUTION FUNCTION ---

def ingest_and_execute(file_path):
    """
    Reads a flat Excel file with a single header row, then inserts each row
    into the database. Puskesmas is treated as the Facility level (level 3).

    Expected Excel format:
      Row 1: Header row with columns: puskesmas, district, shc, nik,
              no_rm_lama, nama_pasien, tgl_lahir, jenis_kelamin, no_telp,
              tanggal_pendaftaran, kunjungan_terakhir, sistole, diastole,
              tgl_terjadwal, tgl_panggilan, jenis_hasil, alasan_dihapus,
              gula_darah, jenis_gula_darah, wilayah
      Optional: tgl_kunjungan_ht, tgl_kunjungan_dm
      Row 2+: Data rows (one patient per row)

    Encounter date priority:
      HTN followup (tgl_kunjungan_ht) → DM followup (tgl_kunjungan_dm)
      → Last visit (kunjungan_terakhir) → Registration (tanggal_pendaftaran)

    When BOTH followup dates are present with BP + BS data,
    two separate encounters are created with their respective dates.
    """

    DTYPE_MAPPING = {'nik': str, 'no_telp': str, 'no_rm_lama': str}

    try:
        df_data = pd.read_excel(
            file_path,
            sheet_name=0,
            header=0,
            dtype=DTYPE_MAPPING,
            engine='calamine'
        )
    except Exception as e:
        print(f"Error loading Excel file: {e}", file=sys.stderr)
        return

    # Normalize column names: lowercase, replace non-alphanumeric with underscore
    df_data.columns = df_data.columns.astype(str).str.lower().str.replace(r'[^a-z0-9_]+', '_', regex=True).str.strip('_')

    print(f"Columns found: {list(df_data.columns)}", file=sys.stderr)
    print(f"Total rows: {len(df_data)}", file=sys.stderr)

    # --- DATABASE EXECUTION ---
    conn = None
    cur = None
    total_processed = 0
    success_inserts = 0

    try:
        conn = psycopg2.connect(**DB_CONNECTION_PARAMS)
        conn.autocommit = True
        cur = conn.cursor()

        # Auto-sync hierarchy_config from HIERARCHY_LEVELS so DB functions
        # (build_drill_url, get_child_level_name) stay in sync automatically.
        for hlvl in HIERARCHY_LEVELS:
            cur.execute("""
                INSERT INTO hierarchy_config (level, display_name, var_name)
                VALUES (%s, %s, %s)
                ON CONFLICT (level) DO UPDATE
                    SET display_name = EXCLUDED.display_name,
                        var_name     = EXCLUDED.var_name
            """, (hlvl['level'], hlvl['display_name'], hlvl['var_name']))

        for record in df_data.to_dict('records'):
            total_processed += 1

            # Skip completely empty rows
            first_key = next(iter(record), None)
            if first_key and pd.isna(record.get(first_key)):
                continue

            # Extract hierarchy from row using HIERARCHY_LEVELS config
            hierarchy = []
            for hlvl in HIERARCHY_LEVELS:
                value = None
                for col in hlvl['column']:
                    value = safe_str(record.get(col))
                    if value:
                        break
                if not value:
                    value = hlvl.get('default')
                if value:
                    hierarchy.append((value, hlvl['level']))

            # org_unit_id is returned by DB function upsert_org_unit_chain()
            org_unit_id = None  # will be set during insertion

            # Clean Sistole and Diastole
            record['sistole'] = clean_blood_pressure(record.get('sistole'))
            record['diastole'] = clean_blood_pressure(record.get('diastole'))

            # Get and clean blood sugar value for validation
            blood_sugar_value = record.get('gula_darah')
            if blood_sugar_value is not None and not pd.isna(blood_sugar_value):
                blood_sugar_value = clean_blood_pressure(blood_sugar_value)
            else:
                blood_sugar_value = None

            blood_sugar_type = safe_str(record.get('jenis_gula_darah'))
            if blood_sugar_value is not None and not blood_sugar_type:
                blood_sugar_type = 'RBS'

            # Parse all date fields
            birth_date_parsed = parse_date_field(record.get('tgl_lahir'))
            registration_date_parsed = parse_date_field(record.get('tanggal_pendaftaran'))
            kunjungan_terakhir_parsed = parse_date_field(record.get('kunjungan_terakhir'))

            # Parse optional followup date columns
            htn_followup_parsed = parse_date_field(record.get('tgl_kunjungan_ht'))
            dm_followup_parsed = parse_date_field(record.get('tgl_kunjungan_dm'))

            # Validate: Skip if registration_date is missing
            if registration_date_parsed is None:
                print(f"\n--- SKIPPING RECORD (NO REGISTRATION DATE) ---", file=sys.stderr)
                print(f"Skipping record #{total_processed} - tanggal_pendaftaran is required", file=sys.stderr)
                continue

            # Encounter date priority: HTN followup → DM followup → Last visit → Registration
            fallback_encounter = (
                kunjungan_terakhir_parsed or
                registration_date_parsed
            )

            scheduled_date_parsed = parse_date_field(record.get('tgl_terjadwal'))
            call_date_parsed = parse_date_field(record.get('tgl_panggilan'))

            # Log the record
            log_record = {k: (None if isinstance(v, float) and pd.isna(v) else v) for k, v in record.items()}
            print(json.dumps(log_record, ensure_ascii=False, default=str))

            # Determine if we have BP and/or BS data
            has_bp = record.get('sistole') is not None or record.get('diastole') is not None
            has_bs = blood_sugar_value is not None

            # --- Per-Row Insertion ---
            try:
                patient_id_sql = to_sql_literal(record.get('nik'), target_type='bigint')

                if patient_id_sql == 'NULL::BIGINT':
                    print(f"\n--- SKIPPING RECORD (NULL patient_id) ---", file=sys.stderr)
                    print(f"Skipping record #{total_processed} due to NULL patient_id (nik)", file=sys.stderr)
                    continue

                # 0. Upsert org_unit hierarchy chain (returns leaf org_unit_id)
                org_unit_id = execute_upsert_org_unit_chain(cur, hierarchy)

                # 1. Upsert patient
                execute_upsert_patient(cur, patient_id_sql, record, registration_date_parsed, birth_date_parsed, org_unit_id)

                # 2. Create encounter(s) and insert clinical data
                if htn_followup_parsed and dm_followup_parsed:
                    # SPLIT: Separate encounters with respective followup dates
                    if has_bp:
                        bp_enc_id = execute_insert_encounter(cur, patient_id_sql, htn_followup_parsed, org_unit_id)
                        execute_insert_bp(cur, bp_enc_id, record.get('sistole'), record.get('diastole'))
                    if has_bs:
                        bs_enc_id = execute_insert_encounter(cur, patient_id_sql, dm_followup_parsed, org_unit_id)
                        execute_insert_bs(cur, bs_enc_id, blood_sugar_type, blood_sugar_value)
                    if not has_bp and not has_bs:
                        # Visit-only encounter when both followup dates present but no clinical data
                        execute_insert_encounter(cur, patient_id_sql, htn_followup_parsed, org_unit_id)
                else:
                    # SINGLE encounter — use priority chain
                    encounter_datetime_parsed = (
                        htn_followup_parsed or
                        dm_followup_parsed or
                        fallback_encounter
                    )
                    # Skip duplicate visit-only encounters when falling back to registration_date
                    is_registration_fallback = (
                        not htn_followup_parsed and
                        not dm_followup_parsed and
                        not kunjungan_terakhir_parsed
                    )
                    if is_registration_fallback and not has_bp and not has_bs:
                        if not encounter_exists(cur, patient_id_sql, encounter_datetime_parsed):
                            execute_insert_encounter(cur, patient_id_sql, encounter_datetime_parsed, org_unit_id)
                    else:
                        enc_id = execute_insert_encounter(cur, patient_id_sql, encounter_datetime_parsed, org_unit_id)
                        execute_insert_bp(cur, enc_id, record.get('sistole'), record.get('diastole'))
                        execute_insert_bs(cur, enc_id, blood_sugar_type, blood_sugar_value)

                success_inserts += 1

                # Insert scheduled_visits if scheduled_date is present
                if scheduled_date_parsed is not None:
                    try:
                        scheduled_visits_sql = f"""
INSERT INTO scheduled_visits (patient_id, scheduled_date, org_unit_id)
VALUES ({patient_id_sql}, {to_sql_literal(scheduled_date_parsed, target_type='DATE')}, {org_unit_id})
ON CONFLICT (patient_id, scheduled_date) DO NOTHING;
"""
                        cur.execute(scheduled_visits_sql)
                    except psycopg2.Error as e:
                        print(f"\n--- SCHEDULED_VISITS INSERT FAILURE ---", file=sys.stderr)
                        print(f"Error inserting scheduled_visits for record #{total_processed}. Details: {e}", file=sys.stderr)

                # Insert call_results if call_date is present
                if call_date_parsed is not None:
                    try:
                        call_results_sql = f"""
INSERT INTO call_results (patient_id, call_date, result_type, removed_reason, org_unit_id)
VALUES ({patient_id_sql}, {to_sql_literal(call_date_parsed, target_type='DATE')}, {to_sql_literal(safe_str(record.get('jenis_hasil')).lower().replace(' ', '_') if safe_str(record.get('jenis_hasil')) else None)}, {to_sql_literal(safe_str(record.get('alasan_dihapus')))}, {org_unit_id})
ON CONFLICT (patient_id, call_date) DO UPDATE SET
  result_type = EXCLUDED.result_type,
  removed_reason = EXCLUDED.removed_reason;
"""
                        cur.execute(call_results_sql)
                    except psycopg2.Error as e:
                        print(f"\n--- CALL_RESULTS INSERT FAILURE ---", file=sys.stderr)
                        print(f"Error inserting call_results for record #{total_processed}. Details: {e}", file=sys.stderr)

            except psycopg2.Error as e:
                print(f"\n--- RECORD FAILURE ---", file=sys.stderr)
                print(f"Error processing record #{total_processed}. Skipping. Details: {e}", file=sys.stderr)

        print(f"\n--- EXECUTION SUMMARY ---", file=sys.stderr)
        print(f"Total records processed: {total_processed}", file=sys.stderr)
        print(f"Successfully inserted records: {success_inserts}", file=sys.stderr)
        print(f"Failed records (skipped): {total_processed - success_inserts}", file=sys.stderr)

    except psycopg2.Error as e:
        print(f"\n--- CONNECTION ERROR ---", file=sys.stderr)
        print(f"PostgreSQL Connection Error: {e}", file=sys.stderr)

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python ingest_file_puskesmas.py <xlsx_file_path>", file=sys.stderr)
    else:
        ingest_and_execute(sys.argv[1])