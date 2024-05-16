import pandas as pd
import argparse
from subprocess import run, CalledProcessError, PIPE
from tqdm import tqdm
import os
import shutil
import ROOT
from array import array
import re

# Extracting the HOLE number from run_description
def extract_hole(description):
    match = re.search(r'HOLE (\d+)', description)
    if match:
        return match.group(1)
    return None
def generate_hadd_run_string(grouped_df, source_folder="NID_source", target_folder="NID_target"):
    """
    Generates a list of strings for each row in the grouped DataFrame, starting with 'hadd target_folder/reco_runX-Y_3D.root'
    where X is the smallest run number and Y is the largest run number in the 'run_number' list,
    followed by all formatted run strings in the 'run_number' list, concatenated and separated by a space.
    Each run string is prepended with 'source_folder/' and follows the format 'reco_runXXXXX_3D.root'.

    Parameters:
    - grouped_df: The grouped DataFrame containing 'HOLE_number', 'DRIFT_V', and 'run_number' (list) columns.
    - source_folder: The folder path to prepend to each individual run file.
    - target_folder: The folder path to prepend to the combined hadd file.

    Returns:
    - A list of strings, where each string begins with 'hadd target_folder/reco_runX-Y_3D.root' and is followed by
      space-separated formatted run numbers for each row, each prepended with 'source_folder/'.
    """
    hadd_run_strings = []
    for index, row in grouped_df.iterrows():
        run_numbers = sorted(row['run_number'])
        start_run = run_numbers[0]
        stop_run = run_numbers[-1]
        # Generate the hadd prefix string with target_folder
        hadd_prefix = f"hadd {target_folder}/reco_run{start_run}-{stop_run}_3D.root"
        # Generate, format, and concatenate each number in the run_numbers list, prepending with source_folder
        run_strings = ' '.join([f'{source_folder}/reco_run{run}_3D.root' for run in run_numbers])
        # Combine the hadd prefix with the concatenated run strings
        complete_string = f"{hadd_prefix} {run_strings}"
        hadd_run_strings.append(complete_string)
    
    return hadd_run_strings
def execute_commands(commands):
    """
    Executes each command in the provided list of commands using a bash shell.
    Stops execution and raises an exception if any command fails.
    Uses tqdm to display a progress bar.

    Parameters:
    - commands: A list of command strings to be executed.
    """
    for command in tqdm(commands, desc="Executing commands"):
        print(f"Executing: {command}")
        try:
            # Execute the command
            result = run(command, shell=True, check=True, stdout=PIPE, stderr=PIPE, text=True)
            # Optionally print the standard output of the command
            print(result.stdout)
        except CalledProcessError as e:
            # If a command fails, print the error and stop execution
            print(f"Error executing {command}: {e.stderr}")
            raise  # Re-raise the exception to stop the script


parser = argparse.ArgumentParser(description='Hadd and in case add env variables to MANGO run from runlog', epilog='Version: 1.0')
parser.add_argument('-log','--logbook',help='Logbook to read', action='store', type=str,default='MANGO_Data Runs.csv')
parser.add_argument('-v','--verbose',help='print more info', action='store_true')
args = parser.parse_args()

df=pd.read_csv(args.logbook)
# Apply the function to create a new column for HOLE number
df['HOLE_number'] = df['run_description'].apply(extract_hole)

# Group by HOLE_number and DRIFT_V and get run_number values
grouped = df.groupby(['HOLE_number', 'DRIFT_V'])['run_number'].apply(list).reset_index()

hadd_strings = generate_hadd_run_string(grouped)
if args.verbose is True:
    for string in hadd_strings:
        print(string)
execute_commands(hadd_strings)