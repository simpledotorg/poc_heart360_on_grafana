import sys
import json
import pandas as pd
from datetime import datetime

# Define the date formats for parsing and output
DATE_FORMAT_IN = "%d-%m-%Y"
DATE_FORMAT_OUT = "%Y-%m-%d"

def get_metadata_from_excel(file_path):
    """
    Parses metadata (header) from the top of an XLSX file using pandas,
    splits 'Tanggal', and converts dates to ISO YYYY-MM-DD format.
    """
    
    # Configuration based on the known file structure:
    # Metadata starts at Line 3 (index 2) and runs for 22 rows.
    ROWS_TO_READ = 22
    START_ROW_INDEX = 2
    
    try:
        # 1. Use pd.read_excel to extract the 22 lines of metadata from the first column.
        df_metadata = pd.read_excel(
            file_path, 
            sheet_name=0,
            header=None,
            skiprows=START_ROW_INDEX,
            nrows=ROWS_TO_READ,
            usecols=[0]
        )
    except FileNotFoundError:
        print(f"Error: File not found at {file_path}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"An unexpected error occurred while reading the Excel file: {e}", file=sys.stderr)
        return None

    metadata = {}

    # 2. Extract key-value pairs into the metadata dictionary
    for index, row in df_metadata.iterrows():
        # The key-value string is in the first (and only) column (index 0)
        line = str(row[0]).strip()
        
        # Split on the first colon (:) only
        parts = line.split(':', 1)
        
        if len(parts) == 2:
            key = parts[0].strip()
            value = parts[1].strip()
            
            # Exclude the title line and handle empty values
            if key and key.lower() != 'laporan harian - pelayanan pasien': 
                metadata[key] = value if value else None
        
    # 3. Process the 'Tanggal' field into start and end dates with format conversion
    if 'Tanggal' in metadata:
        # Remove the original key
        tanggal_value = metadata.pop('Tanggal')
        
        # Split the date string (e.g., "01-01-2025 - 31-01-2025")
        if tanggal_value and ' - ' in tanggal_value:
            try:
                start_date_str, end_date_str = [d.strip() for d in tanggal_value.split(' - ')]
                
                # Convert and format start date (DD-MM-YYYY -> YYYY-MM-DD)
                start_date_dt = datetime.strptime(start_date_str, DATE_FORMAT_IN)
                metadata['dump_start_date'] = start_date_dt.strftime(DATE_FORMAT_OUT)
                
                # Convert and format end date (DD-MM-YYYY -> YYYY-MM-DD)
                end_date_dt = datetime.strptime(end_date_str, DATE_FORMAT_IN)
                metadata['dump_end_date'] = end_date_dt.strftime(DATE_FORMAT_OUT)
                
            except ValueError:
                # Handle cases where the date strings are not in the expected format
                print(f"Warning: Could not parse 'Tanggal' field '{tanggal_value}'. Dates set to None.", file=sys.stderr)
                metadata['dump_start_date'] = None
                metadata['dump_end_date'] = None
        else:
            # If the value is empty or missing the delimiter, assign None
            metadata['dump_start_date'] = None
            metadata['dump_end_date'] = None
            
    return metadata

if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("Usage: python ingest_metadata_excel.py <xlsx_file_path>", file=sys.stderr)
    else:
        result = get_metadata_from_excel(sys.argv[1])
        if result is not None:
            # Output human-readable JSON
            print(json.dumps(result, indent=4, ensure_ascii=False))