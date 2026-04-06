import pandas as pd
import numpy as np
import os

# --- Configuration ---
INPUT_FILE = os.environ.get("INPUT_FILE", "./data/clean_data.csv")
OUTPUT_PATIENTS = os.environ.get("OUTPUT_PATIENTS", "./data/patients_data.csv")
OUTPUT_ENCOUNTERS = os.environ.get("OUTPUT_ENCOUNTERS", "./data/encounters_data.csv")
OUTPUT_BLOOD_PRESSURES = os.environ.get("OUTPUT_BLOOD_PRESSURES", "./data/blood_pressures_data.csv")
OUTPUT_BLOOD_SUGARS = os.environ.get("OUTPUT_BLOOD_SUGARS", "./data/blood_sugars_data.csv")
# ---------------------

def prepare_data_for_postgres():
    """
    Loads complex CSV data, splits multi-valued cells, and transforms it
    into normalized tables (patients, encounters, blood_pressures, blood_sugars) for PostgreSQL.
    """
    if not os.path.exists(INPUT_FILE):
        print(f"Error: Input file '{INPUT_FILE}' not found.")
        return

    # Load the CSV file
    try:
        df = pd.read_csv(INPUT_FILE)
    except Exception as e:
        print(f"Error loading CSV: {e}")
        return

    print("Starting data preparation...")
    
    # 1. PATIENTS TABLE PREPARATION
    
    def get_registration_date(tanggal_str):
        """Extracts the earliest valid date from the Tanggal string."""
        if pd.isna(tanggal_str) or tanggal_str.lower() == 'nan':
            return pd.NaT

        # Clean and split the complex quoted string
        dates = tanggal_str.strip('"').replace('""', '"').strip('"').split('","')
        
        valid_dates = []
        for d in dates:
            d = d.strip('"').strip()
            try:
                # Normalize to date (strip time)
                valid_dates.append(pd.to_datetime(d).normalize())
            except:
                continue
        
        return min(valid_dates) if valid_dates else pd.NaT

    # Calculate registration date for each patient
    df['Registration_Date'] = df['Tanggal'].apply(get_registration_date)

    # Group by patient to create the unique patients table
    patients_df = df.groupby('No MR').agg(
        registration_date=('Registration_Date', 'min'),
        # Using 'Desa' as proxy for both facility and region
        facility=('Desa', 'first'),
        region=('Desa', 'first')
    ).reset_index()

    # Finalize Patients DataFrame structure
    patients_df.rename(columns={'No MR': 'patient_id'}, inplace=True)
    patients_df['patient_status'] = 'ALIVE' 
    patients_df['death_date'] = pd.NaT
    patients_df['patient_id'] = patients_df['patient_id'].astype(str)
    
    patients_df = patients_df[['patient_id', 'patient_status', 'registration_date', 'death_date', 'facility', 'region']]
    patients_df.dropna(subset=['registration_date'], inplace=True) # Remove patients with no valid dates

    print(f"-> Generated {len(patients_df)} unique patient records.")


    # 2. ENCOUNTERS AND BLOOD PRESSURES TABLE PREPARATION

    # Check if blood sugar column exists (try common Indonesian names)
    blood_sugar_col = None
    possible_bs_cols = ['Gula Darah', 'gula_darah', 'Gula darah', 'Blood Sugar', 'blood_sugar']
    for col in possible_bs_cols:
        if col in df.columns:
            blood_sugar_col = col
            break
    
    # Prepare temp dataframe with available columns
    temp_cols = ['No MR', 'Tanggal', 'Tekanan Darah', 'Desa']
    if blood_sugar_col:
        temp_cols.append(blood_sugar_col)
        print(f"-> Found blood sugar column: {blood_sugar_col}")
    else:
        print("-> No blood sugar column found in source data. Blood sugar data will be empty.")
    
    temp_df = df[temp_cols].copy()
    temp_df.rename(columns={'No MR': 'patient_id'}, inplace=True)
    temp_df['patient_id'] = temp_df['patient_id'].astype(str)

    def explode_encounters(row):
        """Splits dates, BP readings, and blood sugar readings and pairs them into individual encounter records."""
        tanggal_str = row['Tanggal']
        bp_str = row['Tekanan Darah']
        bs_str = row.get(blood_sugar_col) if blood_sugar_col else None
        facility = row.get('Desa', 'UNKNOWN')
        
        if pd.isna(tanggal_str) or (pd.isna(bp_str) or bp_str.lower() == 'nan'):
            return []

        # Clean and split Tanggal: handles "date1","date2",...
        tanggal_list = tanggal_str.strip('"').replace('""', '"').strip('"').split('","')
        tanggal_list = [d.strip('"').strip() for d in tanggal_list if d.strip()]

        # Clean and split Tekanan Darah: handles 149/96,149/96,...
        bp_list = bp_str.strip('"').split(',')
        bp_list = [bp.strip() for bp in bp_list if bp.strip()]
        
        # Clean and split blood sugar: handles value1,value2,... or type:value1,type:value2,...
        bs_list = []
        if bs_str and not pd.isna(bs_str) and str(bs_str).lower() != 'nan':
            bs_raw = str(bs_str).strip('"')
            # Handle format: "fasting:120,random:150" or just "120,150"
            bs_list = bs_raw.split(',')
            bs_list = [bs.strip() for bs in bs_list if bs.strip()]
        
        encounters = []
        
        # Process up to the minimum of dates, BP readings, and blood sugar readings
        max_len = max(len(tanggal_list), len(bp_list))
        if bs_list:
            max_len = max(max_len, len(bs_list))
        
        for i in range(min(len(tanggal_list), len(bp_list))):
            date_str = tanggal_list[i]
            bp_reading = bp_list[i]
            
            try:
                # Split and convert BP to integers
                systolic, diastolic = map(int, bp_reading.split('/'))
                encounter_date = pd.to_datetime(date_str).normalize()
                
                # Parse blood sugar if available
                blood_sugar_value = None
                blood_sugar_type = None
                if bs_list and i < len(bs_list):
                    bs_reading = bs_list[i]
                    # Handle format: "fasting:120" or just "120"
                    if ':' in bs_reading:
                        bs_type, bs_value = bs_reading.split(':', 1)
                        blood_sugar_type = bs_type.strip()
                        try:
                            blood_sugar_value = float(bs_value.strip())
                        except:
                            blood_sugar_value = None
                    else:
                        # Just a number, default type to 'random'
                        try:
                            blood_sugar_value = float(bs_reading.strip())
                            blood_sugar_type = 'random'
                        except:
                            blood_sugar_value = None
                
                encounters.append({
                    'patient_id': row['patient_id'],
                    'encounter_date': encounter_date,
                    'facility': facility,
                    'region': facility,  # Using facility as region for now
                    'systolic_bp': systolic,
                    'diastolic_bp': diastolic,
                    'blood_sugar_type': blood_sugar_type,
                    'blood_sugar_value': blood_sugar_value
                })
            except:
                continue # Skip invalid records
                
        return encounters

    # Apply the function and convert the list of dictionaries back to a DataFrame
    all_encounters = temp_df.apply(explode_encounters, axis=1).explode().dropna().tolist()
    
    if not all_encounters:
        encounters_df = pd.DataFrame(columns=['patient_id', 'encounter_date', 'facility', 'region'])
        blood_pressures_df = pd.DataFrame(columns=['patient_id', 'encounter_date', 'systolic_bp', 'diastolic_bp'])
        blood_sugars_df = pd.DataFrame(columns=['patient_id', 'encounter_date', 'blood_sugar_type', 'blood_sugar_value'])
        print("-> Generated 0 encounter records (empty data).")
    else:
        # Create encounters DataFrame (unique by patient_id + encounter_date)
        encounters_list = []
        for enc in all_encounters:
            encounters_list.append({
                'patient_id': enc['patient_id'],
                'encounter_date': enc['encounter_date'],
                'facility': enc['facility'],
                'region': enc['region']
            })
        
        encounters_df = pd.DataFrame(encounters_list)
        # Remove duplicates (same patient, same date)
        encounters_df = encounters_df.drop_duplicates(subset=['patient_id', 'encounter_date'], keep='first')
        encounters_df.reset_index(drop=True, inplace=True)
        
        # Create blood_pressures DataFrame with patient_id and encounter_date for joining
        # The load script will join to get encounter_id from the database
        blood_pressures_list = []
        for enc in all_encounters:
            blood_pressures_list.append({
                'patient_id': enc['patient_id'],
                'encounter_date': enc['encounter_date'],
                'systolic_bp': enc['systolic_bp'],
                'diastolic_bp': enc['diastolic_bp']
            })
        
        blood_pressures_df = pd.DataFrame(blood_pressures_list)
        # Remove duplicates
        blood_pressures_df = blood_pressures_df.drop_duplicates(subset=['patient_id', 'encounter_date'], keep='first')
        
        # Create blood_sugars DataFrame with patient_id and encounter_date for joining
        # The load script will join to get encounter_id from the database
        # Extract blood sugar data from encounters (same pattern as BP)
        blood_sugars_list = []
        for enc in all_encounters:
            if enc.get('blood_sugar_value') is not None:
                blood_sugars_list.append({
                    'patient_id': enc['patient_id'],
                    'encounter_date': enc['encounter_date'],
                    'blood_sugar_type': enc.get('blood_sugar_type'),
                    'blood_sugar_value': enc['blood_sugar_value']
                })
        
        blood_sugars_df = pd.DataFrame(blood_sugars_list)
        # Remove duplicates
        blood_sugars_df = blood_sugars_df.drop_duplicates(subset=['patient_id', 'encounter_date'], keep='first')
        
        print(f"-> Generated {len(encounters_df)} unique encounter records.")
        print(f"-> Generated {len(blood_pressures_df)} blood pressure records.")
        print(f"-> Generated {len(blood_sugars_df)} blood sugar records.")


    # 4. SAVE TO CSV
    patients_df.to_csv(OUTPUT_PATIENTS, index=False, na_rep='NaT') 
    encounters_df.to_csv(OUTPUT_ENCOUNTERS, index=False, na_rep='NaT')
    blood_pressures_df.to_csv(OUTPUT_BLOOD_PRESSURES, index=False, na_rep='NaT')
    blood_sugars_df.to_csv(OUTPUT_BLOOD_SUGARS, index=False, na_rep='NaT')
    
    print("\nData preparation complete:")
    print(f"1. Patient data saved to: {OUTPUT_PATIENTS}")
    print(f"2. Encounters data saved to: {OUTPUT_ENCOUNTERS}")
    print(f"3. Blood pressures data saved to: {OUTPUT_BLOOD_PRESSURES}")
    print(f"4. Blood sugars data saved to: {OUTPUT_BLOOD_SUGARS}")

if __name__ == "__main__":
    prepare_data_for_postgres()
