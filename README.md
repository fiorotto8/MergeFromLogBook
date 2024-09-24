# Merge runs with same description

The repo contains 2 codes:

## `get_info_logbook.py` 

read the excel logbook used for MANGO and merge runs based on some searching criteria (this is done using the *compress* flag). It can also add the environmental information if they are written in the logbook (with the *environment* flag)

## `merge_Runlog.py` 

Read the **Runlog** and the **MIDAS history** to merge the run with the same description (may be changed in the way the script group the dataframe line 254). It creates a hadded root file with an additional TTree containing the environmental parameters, the DRIFT field and the source position in holes.

### Pipeline

- create directories: "source", "target" and "datasets"
- Download reconstructed files to the dataset (tar is better) and uncompressed it to the source folder **DO not remove backup until you have finished**
- Download the Runlog from GRAFANA: <https://grafana.cygno.cloud.infn.it/d/d195dd13-0d21-4ccb-9805-cdcec06a61ff/run-information?orgId=1&refresh=5s>, Inspect -> Data -> Download CSV
- Remove the non interesting runs lines
- Identify the start and stop time of your scan
- On the DAQ machine (with the LNGS VPN on) `ssh cygno01@172.17.19.155` run: `mhist -e MANGOSensors -s YYMMDD[.HHMM[SS]] -p YYMMDD[.HHMM[SS]] > ~/history_output.csv` where -s indicated the start date and -p the end date
- On the local machine run `scp_DAQhistory.sh` and put the remote machine password
- Run `python3 merge_Runlog.py -log name/of/Runlog/downloaded`

### Older version

- you can still use the older version using the `env_log.csv` with the `-env` option
