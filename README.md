# Merge runs with same description

The repo contains 2 codes:

- `get_info_logbook.py` read the excel logbook used for MANGO and merge runs based on some searching criteria (this is done using the *compress* flag). It can also add the environmental information if they are written in the logbook (with the *environment* flag)
- `merge_Runlog.py` read the RunLog csv file (the same of the MARIADB and GRAFANA). It group the runs based on the hole number (source position written in the run description) and the DRIFT_V. If env_log.csv is provided with the flag *env* it can also add the last reading of the environmental parameters in a different TTree named "OtherParam"
  - Put the files to be merged in the `source/` folder
  - use `-log` to specify the `.csv` to use for merging