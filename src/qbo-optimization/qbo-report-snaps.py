import pandas as pd
import gluestick as gs
import os

from datetime import date
from dateutil.relativedelta import relativedelta

INPUT_DIR = "./sync-output"
SNAPSHOT_DIR = "./snapshots"
OUTPUT_DIR = "./etl-output"

# Get the sync_type from the job (full_sync or incremental_sync)
sync_type = os.environ.get("SYNC_TYPE", "incremental_sync")

# By default hotglue gets the last 2 periods + the current period
LOOKBACK_PERIOD = 2

input = gs.Reader()

# Get the report as a dataframe
stream = "GeneralLedgerCashReport"
gl_report = input.get(stream)

# Get the snapped report data
gl_report_snap = gs.read_snapshots(stream, SNAPSHOT_DIR)

# If we have a pre-existing snapshot and new incremental data, we'll need to combine them together
if gl_report_snap is not None and gl_report is not None and sync_type == "incremental_sync":
    # Ensure the Date column is a date
    gl_report_snap["Date"] = pd.to_datetime(gl_report_snap["Date"]).dt.date
    
    # Get today's date
    today = date.today()

    # Get the first day of the month LOOKBACK_PERIOD months ago
    first_day_three_months_ago = (today.replace(day=1) - relativedelta(months=LOOKBACK_PERIOD))
    
    # Drop the last 3 months of data (this is what the incremental sync brought in)
    print(f"Clearing snapped data newer than {first_day_three_months_ago}")
    gl_report_snap = gl_report_snap[gl_report_snap["Date"] < first_day_three_months_ago]
    
    # Combine with gl_report
    print("Combining the incremental data with the snapshot data")
    gl_report = pd.concat([gl_report_snap, gl_report])

# Snapshot the gl_report for future syncs
if gl_report is not None:
    # If the Date column is a date, turn it back to str
    gl_report['Date'] = gl_report['Date']..astype("string")

    # NOTE: We are passing overwrite=True because we are already stitching the snapshot with the incremental data above
    gs.snapshot_records(gl_report, stream, SNAPSHOT_DIR, overwrite=True)

# Write the final output (a full GL Report)
gs.to_export(gl_report, stream, OUTPUT_DIR, export_format="csv")
