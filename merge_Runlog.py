import pandas as pd
import argparse
from subprocess import run, CalledProcessError, PIPE
from tqdm import tqdm
import os
import glob
import shutil
import ROOT
from array import array
import re
import uproot
import numpy as np

# Extracting the HOLE number from run_description
def extract_hole(description):
    match = re.search(r'HOLE (\d+)', description)
    if match:
        return match.group(1)
    return None
def generate_hadd_run_string(grouped_df, source_folder="source", target_folder="target"):
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
def find_nearest_env_data(env_df, start_time):
    """
    Find the row in the env_df with the Timestamp nearest to the provided start_time.

    Parameters:
    - env_df: DataFrame containing the environmental data.
    - start_time: Timestamp to find the nearest entry for.

    Returns:
    - A dictionary containing the environmental data for the nearest Timestamp.
    """
    env_df['Timestamp'] = pd.to_datetime(env_df['Timestamp'], format="%d/%m/%Y_%H-%M-%S",dayfirst=True)
    start_time = pd.to_datetime(start_time,dayfirst=True)
    nearest_row = env_df.iloc[(env_df['Timestamp'] - start_time).abs().argsort()[:1]]
    nearest_data = nearest_row.to_dict(orient='records')[0]
    if 'Timestamp' in nearest_data:
        del nearest_data['Timestamp']
    return nearest_data
def find_nearest_env_data_MIDAS(env_df, start_time):
    """
    Find the row in the env_df with the Timestamp nearest to the provided start_time.

    Parameters:
    - env_df: DataFrame containing the environmental data.
    - start_time: Timestamp to find the nearest entry for.

    Returns:
    - A dictionary containing the environmental data for the nearest Timestamp.
    """
    #print(env_df['Time'])

    # Convert the Time column to string and clean it
    env_df['Time'] = env_df['Time'].astype(str).str.strip()

    # Convert the Time column to datetime format without strict format checking
    env_df['Time'] = pd.to_datetime(env_df['Time'])


    # Convert the start_time to datetime
    start_time = pd.to_datetime(start_time,dayfirst=True, format="%Y-%m-%d %H:%M:%S")
    #print(start_time)
    # Find the row with the nearest timestamp
    nearest_row = env_df.iloc[(env_df['Time'] - start_time).abs().argsort()[:1]]
    
    # Convert the nearest row to a dictionary
    nearest_data = nearest_row.to_dict(orient='records')[0]
    
    # Remove the 'Time' key from the dictionary if it exists
    if 'Time' in nearest_data:
        del nearest_data['Time']
    
    return nearest_data
def update_root_file_with_env_data_NOuprrot( run_number, env_data,source_folder="source"):
    """
    Update the ROOT file with environmental data by adding new branches.

    Parameters:
    - source_folder: The folder path to prepend to the ROOT file.
    - run_number: The run number to identify the ROOT file.
    - env_data: A dictionary containing the environmental data.
    """
    file_name = f"{source_folder}/reco_run{run_number}_3D.root"
    file = ROOT.TFile(file_name, "UPDATE")

    # Check if the file is open
    if file.IsZombie():
        print(f"Error: Cannot open {file_name}")
        return

    tree_name = "Events"  # Replace 'Events' with the actual name of your tree
    tree = file.Get(tree_name)

    # Create arrays to hold the data for the new branches
    keg_t = array('f', [env_data['KEG_temp']])
    keg_p = array('f', [env_data['KEG_pressure']])
    keg_h = array('f', [env_data['KEG_humidity']])
    keg_voc = array('f', [env_data['KEG_voc']])
    source_pos = array('f', [env_data['Source_pos']])
    mangolino_t = array('f', [env_data['MANGOlino_temp']])
    mangolino_p = array('f', [env_data['MANGOlino_pressure']])
    mangolino_h = array('f', [env_data['MANGOlino_humidity']])
    mangolino_voc = array('f', [env_data['MANGOlino_voc']])

    # Create new branches in the tree
    tree.Branch("KEG_temp", keg_t, "KEG_temp/F")
    tree.Branch("KEG_pressure", keg_p, "KEG_pressure/F")
    tree.Branch("KEG_humidity", keg_h, "KEG_humidity/F")
    tree.Branch("KEG_voc", keg_voc, "KEG_voc/F")
    tree.Branch("Source_pos", source_pos, "Source_pos/F")
    tree.Branch("MANGOlino_temp", mangolino_t, "MANGOlino_temp/F")
    tree.Branch("MANGOlino_pressure", mangolino_p, "MANGOlino_pressure/F")
    tree.Branch("MANGOlino_humidity", mangolino_h, "MANGOlino_humidity/F")
    tree.Branch("MANGOlino_voc", mangolino_voc, "MANGOlino_voc/F")

    # Fill the new branches
    for i in range(tree.GetEntries()):
        tree.GetEntry(i)
        tree.Fill()

    # Write changes to the file and close it
    tree.Write("", ROOT.TObject.kOverwrite)
    file.Close()
def add_branch(file_path, value, branch_name):
    """
    Add a new branch with the given name and value to the ROOT file at the specified path.

    Parameters:
    - file_path: The path to the ROOT file.
    - value: The value to add to the new branch.
    - branch_name: The name of the new branch.
    """
    with uproot.update(file_path) as file:
        n_entries = file["Events"].num_entries
        new_data = np.full(n_entries, value, dtype=np.float32)
        file["Events"].new_branch({branch_name: new_data})
def update_root_file_with_env_data(run_number, env_data, source_folder="source"):
    """
    Update the ROOT file with environmental data by adding new branches.

    Parameters:
    - source_folder: The folder path to prepend to the ROOT file.
    - run_number: The run number to identify the ROOT file.
    - env_data: A dictionary containing the environmental data.
    """
    root_file_name = f"reco_run{run_number}_3D.root"
    root_file_path = os.path.join(source_folder, root_file_name)

    # Check if file exists
    if os.path.exists(root_file_path):
        # Prepare values and branch names
        branch_names = list(env_data.keys())
        # Add branches to ROOT file
        for branch in branch_names:
            if branch != "Timestamp":
                value = env_data[branch] if env_data[branch] is not None else 0.0
                add_branch(root_file_path, float(value), branch)
    else:
        print(f"File not found: {root_file_path}")
def add_branch(root_file_path, value,branch_name, tree_name="Events"):
    """
    Adds a new branch to a TTree in a ROOT file containing temperatures.

    Parameters:
    - root_file_path: Path to the ROOT file.
    - tree_name: Name of the TTree to modify.
    - temperatures: List of temperatures to add as a new branch.
    """
    # Open the ROOT file in UPDATE mode
    root_file = ROOT.TFile(root_file_path, "UPDATE")
    tree = root_file.Get(tree_name)
    if not tree:
        print(f"Tree {tree_name} not found in file {root_file_path}")
        return
    # Create a new branch with a single float to hold the temperature
    temp = array('f', [0])  # 'f' is for float, initialize with one element
    new_branch = tree.Branch(branch_name, temp, branch_name+"/F")
    # Fill the new branch with values
    temp[0] = value  # Update the array with the current temperature
    new_branch.Fill()  # Fill the branch for the current entry
    # Write changes and close the file
    tree.Write("", ROOT.TObject.kOverwrite)  # Overwrite the existing tree
    root_file.Close()
def create_other_param_tree(root_file_path, env_data, tree_name="OtherParam"):
    """
    Create a new TTree named 'OtherParam' in the ROOT file and add environmental data as branches.

    Parameters:
    - root_file_path: Path to the ROOT file.
    - env_data: A dictionary containing the environmental data.
    - tree_name: The name of the new TTree to be created.
    """
    # Open the ROOT file in UPDATE mode
    root_file = ROOT.TFile(root_file_path, "UPDATE")
    
    # Create a new TTree
    new_tree = ROOT.TTree(tree_name, tree_name)
    
    # Create arrays to hold the data for the new branches
    branches = {}
    for key, value in env_data.items():
        branches[key] = array('f', [float(value)])
        new_tree.Branch(key, branches[key], f"{key}/F")
    
    # Fill the new tree with the data
    new_tree.Fill()
    
    # Write changes and close the file
    new_tree.Write("", ROOT.TObject.kOverwrite)
    root_file.Close()
def update_root_file_with_new_tree(run_number, env_data, source_folder="source"):
    """
    Update the ROOT file with a new TTree containing environmental data.

    Parameters:
    - source_folder: The folder path to prepend to the ROOT file.
    - run_number: The run number to identify the ROOT file.
    - env_data: A dictionary containing the environmental data.
    """
    root_file_name = f"reco_run{run_number}_3D.root"
    root_file_path = os.path.join(source_folder, root_file_name)

    # Check if file exists
    if os.path.exists(root_file_path):
        create_other_param_tree(root_file_path, env_data)
    else:
        print(f"File not found: {root_file_path}")

parser = argparse.ArgumentParser(description='Hadd and in case add env variables to MANGO run from runlog', epilog='Version: 1.0')
parser.add_argument('-log','--logbook',help='Logbook to read', action='store', type=str,default='MANGO_Data_Runs.csv')
parser.add_argument('-v','--verbose',help='print more info', action='store_true')
parser.add_argument('-env','--env',help='attach environmental variables from log', action='store_true')
args = parser.parse_args()

df=pd.read_csv(args.logbook)
df["run_number"] = df["run_number"].astype(int)
# Apply the function to create a new column for HOLE number
df['HOLE_number'] = df['source_position']

# Group by HOLE_number and DRIFT_V and get run_number values
grouped = df.groupby(['HOLE_number', 'DRIFT_V'])['run_number'].apply(list).reset_index()

print("Attaching env variables...")
if args.env:
    # Read the environmental log data
    env_log_df = pd.read_csv('env_log.csv', delimiter=";")
    # Find the nearest environmental data for each run and update the ROOT file
    for index, row in tqdm(df.iterrows()):
        env_data = find_nearest_env_data(env_log_df, row['start_time'])
        env_data['DRIFT_V'] = row['DRIFT_V']  # Add the DRIFT_V value to env_data
        env_data['HOLE_number'] = row['source_position']  # Add the HOLE_number value to env_data
        #print(env_data)
        # Update ROOT file with the new tree
        update_root_file_with_new_tree(row['run_number'], env_data)
else:
    # Read the CSV file, skipping the first 7 rows, and do not infer headers automatically
    env_log_df = pd.read_csv("history_output.csv", skiprows=8, sep=r'\t+', header=None, dtype=str, engine='python')

    # Manually set the column names
    env_log_df.columns = ['Time', 'KEG_temp', 'KEG_pressure', 'KEG_humidity', 'MANGOlino_temp', 'MANGOlino_pressure', 'MANGOlino_humidity']

    # Find the nearest environmental data for each run and update the ROOT file
    for index, row in df.iterrows():
        env_data = find_nearest_env_data_MIDAS(env_log_df, row['start_time'])
        #print(env_data,df['start_time'])
        #print(row["DRIFT_V"],row["source_position"])
        env_data['DRIFT_V'] = row['DRIFT_V']  # Add the DRIFT_V value to env_data
        env_data['HOLE_number'] = row['source_position']  # Add the HOLE_number value to env_data
        # Update ROOT file with the new tree
        update_root_file_with_new_tree(row['run_number'], env_data)

print("Removing leftovers from target folder...")
files = glob.glob('target/*')
for f in files:
    os.remove(f)

hadd_strings = generate_hadd_run_string(grouped)
if args.verbose is True:
    for string in hadd_strings: print(string)
execute_commands(hadd_strings)
