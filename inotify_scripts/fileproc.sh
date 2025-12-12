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
  # The assumption here is that if python is not installed, no dependency is
  # installed. Ideally, this should be a proper dependencies check for
  # everything we need, but we don't have the luxury of time for such rigor
  echo "Dependencies missing. Installing..."
  apk add ruby ruby-csv
  echo "Dependencies installed."
else
  echo "Dependencies exist."
fi

if [[ "$3" == "MODIFY" ]]; then
  INPUT_FILE=$FULL_PATH BP_OUT="/grafana_target/bp_encounters_data.csv" PATIENTS_OUT="/grafana_target/patients_data.csv" ruby /scripts/transformer/transform.rb
fi
