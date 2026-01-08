import pandas as pd
import gluestick as gs
import os
import json

from datetime import date
from dateutil.relativedelta import relativedelta


# Use environment variables if provided by test framework, otherwise use defaults
INPUT_DIR = os.environ.get("base_input_dir", "./sync-output")
SNAPSHOT_DIR = os.environ.get("snapshot_dir", "./snapshots")
OUTPUT_DIR = os.environ.get("output_dir", "./etl-output")

if not os.path.exists(SNAPSHOT_DIR):
    os.makedirs(SNAPSHOT_DIR)

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)


# By default hotglue gets the last 2 periods + the current period
LOOKBACK_PERIOD = 2

# Get the report periods from the config file
if os.path.exists("config.json"):
    with open("config.json", "r") as f:
        config = json.load(f)
        LOOKBACK_PERIOD = config.get("report_periods", 3) - 1

input = gs.Reader()

# Get the report as a dataframe
stream = "GeneralLedgerAccrualReport"
gl_report = input.get(stream)
has_incremental_data = gl_report is not None and not gl_report.empty
gl_report_snap = gs.read_snapshots(stream, SNAPSHOT_DIR)
has_snapshot_data = gl_report_snap is not None and not gl_report_snap.empty

if has_incremental_data:
    # Give rows missing valid dates a date identifier
    # These incldue Beginning balanace rows
    parsed_dates = pd.to_datetime(gl_report["Date"], errors="coerce", utc=True)
    invalid_date_mask = parsed_dates.isna()
    gl_report.loc[invalid_date_mask, "Date"] = "N/A-" + gl_report.loc[invalid_date_mask, "Date"].astype(str)


# Stitch together new data with snapshots
if has_snapshot_data and has_incremental_data:

    today = date.today()

    first_day_of_lookback_period = (today.replace(day=1) - relativedelta(months=LOOKBACK_PERIOD))

    snapped_date_records = gl_report_snap[pd.to_datetime(gl_report_snap["Date"], errors="coerce", utc=True).notna()]
    new_date_records = gl_report[pd.to_datetime(gl_report["Date"], errors="coerce", utc=True).notna()]

    snapped_non_date_records = gl_report_snap[pd.to_datetime(gl_report_snap["Date"], errors="coerce", utc=True).isna()]
    new_non_date_records = gl_report[pd.to_datetime(gl_report["Date"], errors="coerce", utc=True).isna()]
    non_date_records = pd.concat([snapped_non_date_records, new_non_date_records]).drop_duplicates(subset=["Date", "Account#", "TransactionTypeId", "Categories"])

    # Drop the last N periods of data (this is what the incremental sync brought in)
    snapped_date_records = snapped_date_records[pd.to_datetime(snapped_date_records["Date"], errors="coerce", utc=True) < pd.Timestamp(first_day_of_lookback_period, tz='UTC')]
    snapped_date_records["Date"] = pd.to_datetime(snapped_date_records["Date"], errors="coerce", utc=True).dt.strftime("%Y-%m-%d")

    # Keep the last N periods of data (this is what the incremental sync brought in)
    new_date_records = new_date_records[pd.to_datetime(new_date_records["Date"], errors="coerce", utc=True) >= pd.Timestamp(first_day_of_lookback_period, tz='UTC')]
    new_date_records["Date"] = pd.to_datetime(new_date_records["Date"], errors="coerce", utc=True).dt.strftime("%Y-%m-%d")

    gl_report = pd.concat([snapped_date_records, new_date_records, non_date_records])

    # Convert the Date column to string
    gl_report["Date"] = gl_report["Date"].astype(str)

if gl_report is not None:
    gs.snapshot_records(gl_report, stream, SNAPSHOT_DIR, overwrite=True)
    gs.to_export(gl_report, stream, OUTPUT_DIR, export_format="parquet")
