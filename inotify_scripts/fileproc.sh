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

python3 "${H360TK_DATA_LOAD_SCRIPT}" "$FULL_PATH"
