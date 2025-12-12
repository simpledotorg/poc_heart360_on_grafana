#!/bin/bash

# $1 --> timestamp
# $2 --> watched file/directory path
# $3 --> event name(s)
# $4 --> filename (if a directory is monitored)

echo $1 $2 $3 $4

FULL_PATH="${2}${4}"

if [ ! -f "$FULL_PATH" ]; then
    echo "Info: '$FULL_PATH' is not a regular file or does not exist. Exiting."
    exit 0
fi

echo "We got a file !! $FULL_PATH"

if ! which ruby > /dev/null; then
  # The assumption here is that if ruby is not installed, no dependency is
  # installed. Ideally, this should be a proper dependencies check for
  # everything we need, but we don't have the luxury of time for such rigor
  echo "Dependencies missing. Installing..."
  apk add ruby ruby-csv postgresql-client
  echo "Dependencies installed."
else
  echo "Dependencies exist."
fi

if [[ "$3" == "MODIFY" ]]; then
  export INPUT_FILE=$FULL_PATH

  # TODO: In the case of parallel processing, update the filename here to be
  # based on the input file

  export BP_OUT="/grafana_target/bp_encounters_data.csv"
  export PATIENTS_OUT="/grafana_target/patients_data.csv"

  # generate the raw data for patients and bo_encounters
  echo "Generating Patients and BP data for $FULL_PATH"
  ruby /scripts/transformer/transform.rb

  # load the data into postgres
  echo "Executing automated data copy for patients and encounters..."

  # --- Load Patients Data ---
  PGPASSWORD=$POSTGRES_PASSWORD psql -v ON_ERROR_STOP=1 --host "$POSTGRES_HOST" --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    \COPY patients FROM '$PATIENTS_OUT' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', NULL 'NaT');
EOSQL

  # --- Load BP Encounters Data ---
  PGPASSWORD=$POSTGRES_PASSWORD psql -v ON_ERROR_STOP=1 --host "$POSTGRES_HOST" --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    \COPY bp_encounters FROM '$BP_OUT' WITH (FORMAT CSV, HEADER TRUE, DELIMITER ',', NULL 'NaT');
EOSQL

  echo "Data copy completed successfully."
fi
