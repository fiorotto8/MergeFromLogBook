#!/bin/bash

# Variables
REMOTE_USER="cygno01"
REMOTE_HOST="172.17.19.155"
REMOTE_COMMAND="/home/software//midas/bin/mhist -e MANGOSensors -s 240704.223544 -p 240705.210215"
OUTPUT_FILE="history_output.csv"

# Copy the output file from the remote machine to the local machine
scp ${REMOTE_USER}@${REMOTE_HOST}:~/${OUTPUT_FILE} .

# Notify the user
echo "Output has been copied to ${OUTPUT_FILE}"