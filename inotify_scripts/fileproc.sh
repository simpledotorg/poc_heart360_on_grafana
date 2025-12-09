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

if ! which python3 > /dev/null; then
  # The assumption here is that if python is not installed, no dependency is
  # installed. Ideally, this should be a proper dependencies check for
  # everything we need, but we don't have the luxury of time for such rigor
  echo "Python 3 doesn't exist yet. Installing..."
  apk add python3 py3-pip py3-pandas
  echo "Python 3 and Pip installed."
else
  echo "Python 3 already exists."
fi

if [[ "$3" == "MODIFY" ]]; then
  INPUT_FILE=$FULL_PATH python prepare_date.py
fi
