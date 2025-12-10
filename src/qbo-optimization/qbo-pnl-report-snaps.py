import pandas as pd
import gluestick as gs
import os
import json

from datetime import date
from dateutil.relativedelta import relativedelta

# Get the sync_type (full_sync or incremental_sync)
sync_type = os.environ.get("SYNC_TYPE", "incremental_sync")

INPUT_DIR = "./sync-output"
SNAPSHOT_DIR = "./snapshots"
OUTPUT_DIR = "./etl-output"

# By default hotglue gets the last 2 periods + the current period
LOOKBACK_PERIODS = 2

# Get the report periods from the config file
with open("config.json", "r") as f:
    config = json.load(f)
    LOOKBACK_PERIODS = config.get("report_periods", 3) - 1


input = gs.Reader()


# Get the report as a dataframe
stream = "ProfitAndLossDetailReport"
pnl_report = input.get(stream, catalog_types=True)


# Get the snapped report data
pnl_report_snap = gs.read_snapshots(stream, SNAPSHOT_DIR)

# If we have a pre-existing snapshot and new incremental data, we'll need to combine them together
snapshot_exists = pnl_report_snap is not None and not pnl_report_snap.empty

if snapshot_exists and pnl_report is not None and sync_type == "incremental_sync":
    # Ensure the Date column is a date
    pnl_report_snap["Date"] = pd.to_datetime(pnl_report_snap["Date"]).dt.date
    
    # Get today's date
    today = date.today()

    # Get the first day of the month LOOKBACK_PERIOD months ago
    first_day_of_lookback_period = (today.replace(day=1) - relativedelta(months=LOOKBACK_PERIODS))
    
    # Drop the last 3 months of data (this is what the incremental sync brought in)
    print(f"Clearing snapped data newer than {first_day_of_lookback_period}")
    pnl_report_snap = pnl_report_snap[pnl_report_snap["Date"] < first_day_of_lookback_period]

    # Convert snapped dates back to dt for consistency with sync output
    pnl_report_snap["Date"] = pd.to_datetime(pnl_report_snap["Date"]).astype('datetime64[ns]')
    
    # Combine with pnl_report
    print("Combining the incremental data with the snapshot data")
    pnl_report = pd.concat([pnl_report_snap, pnl_report])

if pnl_report is not None:
    # If the Date column is a date, turn it back to str
    pnl_report['Date'] = pnl_report['Date'].astype("string")

    # NOTE: We are passing overwrite=True because we are already stitching the snapshot with the incremental data above
    gs.snapshot_records(pnl_report, stream, SNAPSHOT_DIR, overwrite=True)

# Write the final output (a full PNL Report)
gs.to_export(pnl_report, stream, OUTPUT_DIR, export_format="parquet")

