import sys
import json
import pandas as pd
from datetime import datetime
import re

# --- CONFIGURATION (WHITELIST) ---
# Whitelist of final columns to be included in the output JSON records.
FINAL_COLUMN_WHITELIST = [
    'Puskesmas', 
    'dump_start_date', 
    'dump_end_date',
    'nik', 
    'nama_pasien',
    'jenis_kelamin',
    'tgl_lahir'
    'tanggal', 
    'sistole', 
    'diastole'
]
# ---------------------------------

# Define the date formats for parsing and output
DATE_FORMAT_IN = "%d-%m-%Y"
DATE_FORMAT_OUT = "%Y-%m-%d"

def clean_blood_pressure(value):
    """
    Strips non-numeric characters (units like 'mm', 'Hg') from a string
    and converts the result to a float. Returns None if the input is invalid or NaN.
    """
    if pd.isna(value) or value is None:
        return None
    
    # 1. Convert to string and strip surrounding whitespace
    s = str(value).strip()
    
    # 2. Use regex to remove all characters that are NOT a digit (0-9) or a period (.)
    cleaned_s = re.sub(r'[^\d.]', '', s)
    
    # 3. Attempt conversion to float
    try:
        # Check if the cleaned string is empty or just a period
        if not cleaned_s or cleaned_s == '.':
            return None
        return float(cleaned_s)
    except ValueError:
        return None

def get_metadata_from_excel(file_path):
    """
    Extracts and formats the static metadata block (lines 3-24), 
    including date conversion and field splitting.
    """
    # Metadata starts at Line 3 (index 2) and runs for 22 rows.
    ROWS_TO_READ = 22
    START_ROW_INDEX = 2
    
    try:
        df_metadata = pd.read_excel(
            file_path, 
            sheet_name=0,
            header=None,
            skiprows=START_ROW_INDEX,
            nrows=ROWS_TO_READ,
            usecols=[0]
        )
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
                
                # Convert DD-MM-YYYY to YYYY-MM-DD
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

def stream_ingestion_output(file_path):
    """
    1. Extracts and prints the static metadata JSON (linearized).
    2. Loads the main tabular data, cleans BP fields, combines with metadata, 
       and prints each row as a separate, filtered, linearized JSON object.
    """
    
    # 1. Extract and process static metadata
    try:
        static_metadata = get_metadata_from_excel(file_path)
    except Exception as e:
        print(f"Error during metadata extraction: {e}", file=sys.stderr)
        return

    # Print the single header JSON (linearized)
    print(json.dumps(static_metadata, ensure_ascii=False))

    # 2. Load the main tabular data (data header is at line 26, index 25)
    SKIP_ROWS_TO_DATA_HEADER = 25 
    
    try:
        df_data = pd.read_excel(
            file_path, 
            sheet_name=0,
            header=0,
            skiprows=SKIP_ROWS_TO_DATA_HEADER
        )
    except Exception as e:
        print(f"Error loading main data table: {e}", file=sys.stderr)
        return

    # Clean column names: lowercase, snake_case
    df_data.columns = df_data.columns.astype(str).str.lower().str.replace(r'[^a-z0-9_]+', '_', regex=True).str.strip('_')
    
    # 3. Combine metadata and data rows and stream filtered output
    
    # Iterate through the DataFrame rows
    for record in df_data.to_dict('records'):
        # Skip rows where the first column is NaN
        first_key = next(iter(record), None)
        if first_key and pd.isna(record.get(first_key)):
             continue
            
        # Start with the static metadata
        flat_record = static_metadata.copy()
        
        # Merge the dynamic row data
        flat_record.update(record)

        # --- NEW LOGIC: Clean Sistole and Diastole ---
        flat_record['sistole'] = clean_blood_pressure(flat_record.get('sistole'))
        flat_record['diastole'] = clean_blood_pressure(flat_record.get('diastole'))
        # ---------------------------------------------
        
        # Filter the final record against the configurable whitelist
        final_filtered_record = {
            key: None if pd.isna(flat_record.get(key)) else flat_record.get(key)
            for key in FINAL_COLUMN_WHITELIST 
            if key in flat_record 
        }
        
        # Print the filtered data row JSON (linearized)
        print(json.dumps(final_filtered_record, ensure_ascii=False))


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python ingest_excel_data_stream.py <xlsx_file_path>", file=sys.stderr)
    else:
        stream_ingestion_output(sys.argv[1])