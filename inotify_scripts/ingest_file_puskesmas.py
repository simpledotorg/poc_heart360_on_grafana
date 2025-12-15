import sys
import json
import pandas as pd
from datetime import datetime
import re
import os
import psycopg2
from typing import Dict, Any

# --- CONFIGURATION (WHITELIST) ---
FINAL_COLUMN_WHITELIST = [
    'Puskesmas', 
    'dump_start_date', 
    'dump_end_date', 
    'nik', 
    'no_rm_lama', 
    'sistole', 
    'diastole',
    'nama_pasien',
    'tgl_lahir',
    'tanggal' 
]
# ---------------------------------

# Define the date formats for parsing and output
DATE_FORMAT_IN = "%d-%m-%Y"
DATE_FORMAT_OUT = "%Y-%m-%d"

# --- DATABASE CONNECTION DETAILS ---
DB_CONNECTION_PARAMS = {
    'host': os.getenv('POSTGRES_HOST', 'postgres'),
    'database': os.getenv('POSTGRES_DB', 'metrics_db'),
    'user': os.getenv('POSTGRES_USER', 'grafana_user'),
    'password': os.getenv('POSTGRES_PASSWORD', 'your_db_password'),
}
SP_REGION_VALUE = 'Demo'
# ----------------------------------------------------------------

# --- HELPER FUNCTIONS ---
def clean_blood_pressure(value):
    """
    Strips non-numeric characters (units like 'mm', 'Hg') from a string
    and converts the result to a float. Returns None if the input is invalid or NaN.
    """
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

def get_metadata_from_excel(file_path):
    """
    Extracts and formats the static metadata block (lines 3-24).
    """
    ROWS_TO_READ = 22
    START_ROW_INDEX = 2
    
    try:
        df_metadata = pd.read_excel(file_path, sheet_name=0, header=None, skiprows=START_ROW_INDEX, nrows=ROWS_TO_READ, usecols=[0])
    except Exception as e:
        raise Exception(f"Failed to read metadata block: {e}")

    metadata = {}
    for index, row in df_metadata.iterrows():
        line = str(row[0]).strip()
        parts = line.split(':', 1)
        if len(parts) == 2:
            key = parts[0].strip()
            value = parts[1].strip()
            if key and key.lower() != 'laporan harian - pelayanan pasien': 
                metadata[key] = value if value else None
    
    # Process 'Tanggal' field (Split and format date)
    if 'Tanggal' in metadata:
        tanggal_value = metadata.pop('Tanggal')
        if tanggal_value and ' - ' in tanggal_value:
            try:
                start_date_str, end_date_str = [d.strip() for d in tanggal_value.split(' - ')]
                start_date_dt = datetime.strptime(start_date_str, DATE_FORMAT_IN)
                metadata['dump_start_date'] = start_date_dt.strftime(DATE_FORMAT_OUT)
                end_date_dt = datetime.strptime(end_date_str, DATE_FORMAT_IN)
                metadata['dump_end_date'] = end_date_dt.strftime(DATE_FORMAT_OUT)
            except ValueError:
                metadata['dump_start_date'] = None
                metadata['dump_end_date'] = None
        else:
            metadata['dump_start_date'] = None
            metadata['dump_end_date'] = None
            
    return metadata

def generate_sql_insert_statement(record: Dict[str, Any], facility: str, region: str) -> str:
    """
    Generates the PostgreSQL function call string for a single data record, 
    ensuring schema qualification and explicit type casting.
    """
    
    def to_sql_literal(value, target_type=None):
        if value is None or pd.isna(value):
            return 'NULL'
        
        if target_type == 'bigint':
            val_str = str(value).strip()
            if val_str.endswith('.0'):
                val_str = val_str[:-2]
            if not val_str.isdigit():
                return 'NULL'
            return f"CAST('{val_str}' AS bigint)"
            
        if target_type == 'DATE' and isinstance(value, (datetime, pd.Timestamp)):
            return f"'{value.strftime('%Y-%m-%d')}'::DATE"
            
        if target_type == 'TIMESTAMP' and isinstance(value, (datetime, pd.Timestamp)):
            return f"'{value.strftime('%Y-%m-%d %H:%M:%S')}'::timestamp"

        if isinstance(value, str):
            # FIXED: Use escape sequence for quotes inside the f-string literal
            return f"'{value.replace("'", "''")}'"
        
        if isinstance(value, (int, float)):
            return str(value)
        
        return f"'{str(value).replace("'", "''")}'"

    patient_id_sql = to_sql_literal(record.get('nik'), target_type='bigint')
    birth_date_sql = to_sql_literal(record.get('tgl_lahir'), target_type='DATE')
    encounter_datetime_sql = to_sql_literal(record.get('tanggal'), target_type='TIMESTAMP')


    sql_call = f"""
SELECT public.insert_heart360_data(
    p_patient_id => {patient_id_sql},
    p_patient_name => {to_sql_literal(record.get('nama_pasien'))},
    p_birth_date => {birth_date_sql},
    p_facility => {to_sql_literal(facility)},
    p_region => {to_sql_literal(region)},
    p_encounter_datetime => {encounter_datetime_sql},
    p_diastolic_bp => {to_sql_literal(record.get('diastole'))},
    p_systolic_bp => {to_sql_literal(record.get('sistole'))}
);
"""
    return sql_call.strip()

# --- MAIN INGESTION AND EXECUTION FUNCTION ---

def ingest_and_execute(file_path):
    """
    Main function to read data, generate SQL, and execute against the DB.
    """
    
    # 1. Extract and process static metadata
    try:
        static_metadata = get_metadata_from_excel(file_path)
        facility = static_metadata.get('Puskesmas', 'UNKNOWN')
    except Exception as e:
        print(f"Error during metadata extraction: {e}", file=sys.stderr)
        return

    # Print header JSON for logging/debugging (linearized)
    print(json.dumps(static_metadata, ensure_ascii=False))

    # 2. Load the main tabular data
    SKIP_ROWS_TO_DATA_HEADER = 25 
    
    # Force 'nik' column to be read as string
    DTYPE_MAPPING = {'nik': str} 
    
    try:
        df_data = pd.read_excel(
            file_path, 
            sheet_name=0,
            header=0,
            skiprows=SKIP_ROWS_TO_DATA_HEADER,
            dtype=DTYPE_MAPPING
        )
    except Exception as e:
        print(f"Error loading main data table: {e}", file=sys.stderr)
        return

    # Clean column names: lowercase, snake_case
    df_data.columns = df_data.columns.astype(str).str.lower().str.replace(r'[^a-z0-9_]+', '_', regex=True).str.strip('_')
    
    # --- DATABASE EXECUTION ---
    conn = None
    cur = None
    total_inserts = 0
    
    try:
        conn = psycopg2.connect(**DB_CONNECTION_PARAMS)
        cur = conn.cursor()
        
        # 3. Combine metadata, clean data, and execute SQL for each row
        
        for record in df_data.to_dict('records'):
            first_key = next(iter(record), None)
            if first_key and pd.isna(record.get(first_key)):
                 continue
                
            flat_record = record
            
            # Clean Sistole and Diastole
            flat_record['sistole'] = clean_blood_pressure(flat_record.get('sistole'))
            flat_record['diastole'] = clean_blood_pressure(flat_record.get('diastole'))
            
            # --- Display the final JSON record before generating SQL ---
            final_filtered_record = {
                key: None if pd.isna(flat_record.get(key)) else flat_record.get(key)
                for key in FINAL_COLUMN_WHITELIST 
                if key in flat_record 
            }
            # Print the data row JSON (linearized)
            print(json.dumps(final_filtered_record, ensure_ascii=False))
            # --- END Display Logic ---
            
            # Generate and Execute SQL
            sql_statement = generate_sql_insert_statement(flat_record, facility, SP_REGION_VALUE)
            
            cur.execute(sql_statement)
            total_inserts += 1

        conn.commit()
        print(f"\n--- SUCCESS ---", file=sys.stderr)
        print(f"Successfully processed and inserted {total_inserts} records into the database.", file=sys.stderr)

    except psycopg2.Error as e:
        if conn:
            conn.rollback()
        print(f"\n--- DATABASE ERROR ---", file=sys.stderr)
        print(f"PostgreSQL Error: {e}", file=sys.stderr)
        print("Transaction rolled back.", file=sys.stderr)
        
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python ingest_excel_data_stream.py <xlsx_file_path>", file=sys.stderr)
    else:
        ingest_and_execute(sys.argv[1])