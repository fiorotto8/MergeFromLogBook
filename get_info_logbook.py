import pandas as pd
import argparse
from subprocess import run, CalledProcessError, PIPE
from tqdm import tqdm
import os
import shutil
import ROOT
from array import array

def empty_folder(folder_path):
    """
    Empties all contents of the specified folder without deleting the folder itself.

    Parameters:
    - folder_path: The path to the folder to be emptied.
    """
    for item in os.listdir(folder_path):
        item_path = os.path.join(folder_path, item)
        try:
            if os.path.isfile(item_path) or os.path.islink(item_path):
                os.unlink(item_path)  # Remove files and links
            elif os.path.isdir(item_path):
                shutil.rmtree(item_path)  # Remove directories
        except Exception as e:
            print(f'Failed to delete {item_path}. Reason: {e}')

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

def generate_hadd_run_string(df, source_folder="NID_source", target_folder="NID_target"):
    """
    Generates a list of strings for each row in the DataFrame, starting with 'hadd target_folder/reco_runX-Y_3D.root'
    where X is the start run and Y is the stop run, followed by all formatted run strings from 'StartRun'
    to 'StopRun', concatenated and separated by a space. Each run string is prepended with 'source_folder/'
    and follows the format 'reco_runXXXXX_3D.root'.

    Parameters:
    - df: The DataFrame containing the 'StartRun' and 'StopRun' columns.
    - source_folder: The folder path to prepend to each individual run file.
    - target_folder: The folder path to prepend to the combined hadd file.

    Returns:
    - A list of strings, where each string begins with 'hadd target_folder/reco_runX-Y_3D.root' and is followed by
      space-separated formatted run numbers for each row, each prepended with 'source_folder/'.
    """
    hadd_run_strings = []
    for index, row in df.iterrows():
        start_run = int(row['StartRun'])
        stop_run = int(row['StopRun'])
        # Generate the hadd prefix string with target_folder
        hadd_prefix = f"hadd {target_folder}/reco_run{start_run}-{stop_run}_3D.root"
        # Generate, format, and concatenate each number in the range, prepending with source_folder
        run_strings = ' '.join([f'{source_folder}/reco_run{run}_3D.root' for run in range(start_run, stop_run + 1)])
        # Combine the hadd prefix with the concatenated run strings
        complete_string = f"{hadd_prefix} {run_strings}"
        hadd_run_strings.append(complete_string)
    
    return hadd_run_strings

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

parser = argparse.ArgumentParser(description='Analyze waveform from a certain Run', epilog='Version: 1.0')
parser.add_argument('-c','--compress',help='compress multiple run in single run file', action='store', type=int,default=None)
parser.add_argument('-t','--target',help='target folder where the compressed run are stored', action='store', type=str,default="NID_target")
parser.add_argument('-s','--source',help='source folder where the bare run are stored', action='store', type=str,default="NID_source")
parser.add_argument('-env','--environment',help='append new branch to compress run with run variables', action='store', type=int,default=None)
parser.add_argument('-log','--logbook',help='Logbook to read', action='store', type=str,default='MANGO_LNGS_Logbook.xlsx')
parser.add_argument('-v','--verbose',help='write something to print more info', action='store', type=int,default=None)
args = parser.parse_args()

df = pd.read_excel(args.logbook)
#print(df)

#Select NIF data
str_condition='NID'
NID_condition=df['comments'].str.contains(str_condition, na=False)
NID_selected=df[NID_condition]
if args.verbose is not None:
    print("Selected rows only if they contain:",str_condition)
    print("mask is:",NID_condition)

#start stop column
start = NID_selected['Run number start'].astype(int)
stop = NID_selected['Run number end'].astype(int)
#Drift filed column
driftField=NID_selected["Requested_Drift_field_V_cm"]
#Position column
position=NID_selected["Position of source [hole]"]
#enviromental variable column
temp_df = NID_selected['Sensor inside [T;P;H;--] [K,Pa,%,-]'].str.split(';', expand=True)
temp_df.columns = ['Temperature (K)', 'Pressure (Pa)', 'Humidity (%)', 'VOC (-)']
#Gas
temp_gas=NID_selected["He/CF4 ratio"].str.split('/', expand=True)
temp_gas.columns = ['helium', 'CF4', 'SF6']

# Combine the individual series and DataFrame into a new DataFrame
new_df = pd.DataFrame({
    'StartRun': start,
    'StopRun': stop,
    'He(%)': temp_gas["helium"],
    'CF4(%)': temp_gas["CF4"],
    'SF6(%)': temp_gas["SF6"],
    'Drift Field (Vcm)': driftField,
    'Position': position,
    'Temperature (K)': temp_df['Temperature (K)'],
    'Pressure (Pa)': temp_df['Pressure (Pa)'],
    'Humidity (%)': temp_df['Humidity (%)'],
    'VOC (-)': temp_df['VOC (-)']
})
new_df_reset = new_df.reset_index(drop=True)
if args.verbose is not None: print(new_df_reset)

# Save the DataFrame to a CSV file
new_df_reset.to_csv('df_out.csv', index=False)

if args.compress is not None:
    empty_folder(args.target)
    string_to_run=generate_hadd_run_string(new_df_reset,args.source,args.target)
    if args.verbose is not None: print(string_to_run)
    execute_commands(string_to_run)

if args.environment is not None:
    # Iterate over DataFrame rows
    for index, row in tqdm(new_df_reset.iterrows()):
        # Construct ROOT file name based on StartRun and StopRun
        root_file_name = f"reco_run{row['StartRun']}-{row['StopRun']}_3D.root"
        root_file_path = os.path.join(args.target, root_file_name)

        # Check if file exists
        if os.path.exists(root_file_path):
            # Prepare values and branch names
            values = {col: row[col] for col in new_df_reset.columns if col not in ['StartRun', 'StopRun']}
            branch_names = list(values.keys())
            # Add branches to ROOT file
            for branch in branch_names:
                #print(float(values[branch]), branch)
                if values[branch] is not None: add_branch(root_file_path, float(values[branch]), branch)
                else: add_branch(root_file_path, 0., branch)
        else:
            print(f"File not found: {root_file_path}")