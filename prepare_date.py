import pandas as pd
import numpy as np
import os

# --- Configuration ---
INPUT_FILE = os.environ.get("INPUT_FILE", "./data/clean_data.csv")
OUTPUT_PATIENTS = os.environ.get("OUTPUT_PATIENTS", "./data/patients_data.csv")
OUTPUT_ENCOUNTERS = os.environ.get("OUTPUT_ENCOUNTERS", "./data/bp_encounters_data.csv")
# ---------------------

def prepare_data_for_postgres():
    """
    Loads complex CSV data, splits multi-valued cells, and transforms it
    into two normalized tables (patients and bp_encounters) for PostgreSQL.
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


    # 2. BP ENCOUNTERS TABLE PREPARATION

    temp_df = df[['No MR', 'Tanggal', 'Tekanan Darah']].copy()
    temp_df.rename(columns={'No MR': 'patient_id'}, inplace=True)
    temp_df['patient_id'] = temp_df['patient_id'].astype(str)

    def explode_encounters(row):
        """Splits dates and BP readings and pairs them into individual encounter records."""
        tanggal_str = row['Tanggal']
        bp_str = row['Tekanan Darah']
        
        if pd.isna(tanggal_str) or pd.isna(bp_str) or bp_str.lower() == 'nan':
            return []

        # Clean and split Tanggal: handles "date1","date2",...
        tanggal_list = tanggal_str.strip('"').replace('""', '"').strip('"').split('","')
        tanggal_list = [d.strip('"').strip() for d in tanggal_list if d.strip()]

        # Clean and split Tekanan Darah: handles 149/96,149/96,...
        bp_list = bp_str.strip('"').split(',')
        bp_list = [bp.strip() for bp in bp_list if bp.strip()]
        
        encounters = []
        
        for i in range(min(len(tanggal_list), len(bp_list))):
            date_str = tanggal_list[i]
            bp_reading = bp_list[i]
            
            try:
                # Split and convert BP to integers
                systolic, diastolic = map(int, bp_reading.split('/'))
                encounter_date = pd.to_datetime(date_str).normalize()
                
                encounters.append({
                    'patient_id': row['patient_id'],
                    'encounter_date': encounter_date,
                    'systolic_bp': systolic,
                    'diastolic_bp': diastolic
                })
            except:
                continue # Skip invalid records
                
        return encounters

    # Apply the function and convert the list of dictionaries back to a DataFrame
    all_encounters = temp_df.apply(explode_encounters, axis=1).explode().dropna().tolist()
    bp_encounters_df = pd.DataFrame(all_encounters)
    
    # Check if the dataframe is empty before assigning index
    if bp_encounters_df.empty:
        bp_encounters_df = pd.DataFrame(columns=['patient_id', 'encounter_date', 'systolic_bp', 'diastolic_bp'])
        print("-> Generated 0 encounter records (empty data).")
    else:
        # Add unique encounter ID
        bp_encounters_df.reset_index(drop=True, inplace=True)
        bp_encounters_df['encounter_id'] = bp_encounters_df.index + 1
        bp_encounters_df['patient_id'] = bp_encounters_df['patient_id'].astype(str)
        print(f"-> Generated {len(bp_encounters_df)} BP encounter records.")


    # Finalize BP Encounters DataFrame structure
    bp_encounters_df = bp_encounters_df[['encounter_id', 'patient_id', 'encounter_date', 'diastolic_bp', 'systolic_bp']]


    # 3. SAVE TO CSV
    patients_df.to_csv(OUTPUT_PATIENTS, index=False, na_rep='NaT') 
    bp_encounters_df.to_csv(OUTPUT_ENCOUNTERS, index=False, na_rep='NaT')
    
    print("\nData preparation complete:")
    print(f"1. Patient data saved to: {OUTPUT_PATIENTS}")
    print(f"2. BP Encounters data saved to: {OUTPUT_ENCOUNTERS}")

if __name__ == "__main__":
    prepare_data_for_postgres()
