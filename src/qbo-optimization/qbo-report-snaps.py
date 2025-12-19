import pandas as pd
import gluestick as gs
import os
import json

from datetime import date
from dateutil.relativedelta import relativedelta


sync_type = os.environ.get("SYNC_TYPE", "incremental_sync")

INPUT_DIR = "./sync-output"
SNAPSHOT_DIR = "./snapshots"
OUTPUT_DIR = "./etl-output"

LOOKBACK_PERIOD = 2

# get tenant-config flags
config_path = f'{SNAPSHOT_DIR}/tenant-config.json'

config = {}
if os.path.exists(config_path):
    with open(config_path) as f:
        config = json.load(f)


input = gs.Reader()

# Get the report as a dataframe
stream = "GeneralLedgerCashReport"
gl_report = input.get(stream)


# Get the snapped report data
gl_report_snap = gs.read_snapshots(stream, SNAPSHOT_DIR)

if gl_report is not None and not gl_report.empty and sync_type == 'full_sync':
    # Update records with "Beginning Balance" as Date to Memo
    gl_report.loc[gl_report["Date"] == "Beginning Balance", "Memo"] = "Beginning Balance"
    
    # For each account with "Beginning Balance", calculate the replacement date
    # based on the account's minimum transaction date - 3 months -> prior month last day
    accounts_with_bb = gl_report[gl_report["Date"] == "Beginning Balance"]["AccountId"].unique()
    
    for account_id in accounts_with_bb:
        # Get all transactions for this account (excluding Beginning Balance)
        account_transactions = gl_report[
            (gl_report["AccountId"] == account_id) & 
            (gl_report["Date"] != "Beginning Balance")
        ]
        
        # Get the minimum date for this account
        min_date = account_transactions["Date"].min()
        # Calculate: min_date -> 3 months ago -> last day of prior month
        look_back_period_months_ago = pd.to_datetime(min_date) - relativedelta(months=LOOKBACK_PERIOD + 1)
        beginning_balance_date = (look_back_period_months_ago.replace(day=1) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")

        if beginning_balance_date is not None:
            # Update the Beginning Balance records for this account
            mask = (gl_report["AccountId"] == account_id) & (gl_report["Date"] == "Beginning Balance")
            gl_report.loc[mask, "Memo"] = "Beginning Balance"
            gl_report.loc[mask, "Date"] = beginning_balance_date
            
    gl_report['Date'] = gl_report['Date'].astype("string")

    gs.snapshot_records(gl_report, stream, SNAPSHOT_DIR, overwrite=True, use_csv=True)

# If we have a pre-existing snapshot and new incremental data, we'll need to combine them together
if gl_report_snap is not None and gl_report is not None and not gl_report.empty and sync_type == "incremental_sync":
    """
    Sliding window logic for incremental sync:
    1. The incremental sync brings the last LOOKBACK_PERIOD + 1 months of data (e.g., 3 months)
    2. We need to remove that overlapping period from the snapshot to avoid duplicates
    3. For "Beginning Balance" records, we deduplicate by (AccountId, Memo) and keep the snapshot version
    """
    
    # Mark any "Beginning Balance" records in the new incremental data with Memo = "Beginning Balance"
    gl_report.loc[gl_report["Date"] == "Beginning Balance", "Memo"] = "Beginning Balance"
    
    # Get the set of AccountIds that have "Beginning Balance" in the snapshot
    snapshot_bb_account_ids = set(
        gl_report_snap[gl_report_snap["Memo"] == "Beginning Balance"]["AccountId"].unique()
    )
    
    # Remove "Beginning Balance" records from gl_report for accounts that already have them in the snapshot
    # This keeps the snapshot's "Beginning Balance" record (which has the correct calculated date)
    gl_report_filtered = gl_report[
        ~(
            (gl_report["Memo"] == "Beginning Balance") & 
            (gl_report["AccountId"].isin(snapshot_bb_account_ids))
        )
    ]
    
    # convert the Date column to datetime
    gl_report_filtered["Date"] = pd.to_datetime(gl_report_filtered["Date"], errors="coerce")
    
    min_date_from_new_sync = gl_report_filtered["Date"].min()
    if min_date_from_new_sync.tzinfo is None:
        min_date_from_new_sync = pd.to_datetime(min_date_from_new_sync).tz_localize('UTC')

    gl_report_snap_copy = gl_report_snap.copy()
    
    # Separate "Beginning Balance" records (identified by Memo) from regular transaction records
    snapshot_bb_records = gl_report_snap_copy[gl_report_snap_copy["Memo"] == "Beginning Balance"]
    snapshot_regular_records = gl_report_snap_copy[gl_report_snap_copy["Memo"] != "Beginning Balance"]
    
    # For regular records, filter out the sliding window period (data that the incremental sync just brought in)
    snapshot_regular_records = snapshot_regular_records.copy()
    snapshot_regular_records["Date"] = pd.to_datetime(snapshot_regular_records["Date"], errors="coerce")
    if snapshot_regular_records["Date"].dt.tz is None:
        snapshot_regular_records["Date"] = snapshot_regular_records["Date"].dt.tz_localize('UTC')
    snapshot_regular_filtered = snapshot_regular_records[snapshot_regular_records["Date"] < min_date_from_new_sync]

    # Combine: snapshot Beginning Balance + snapshot records before cutoff + new incremental data
    gl_report = pd.concat([snapshot_bb_records, snapshot_regular_filtered, gl_report_filtered], ignore_index=True)
    
    # Ensure Date column is string type for consistency
    gl_report['Date'] = gl_report['Date'].astype("string")
    
    # Save the combined snapshot
    gs.snapshot_records(gl_report, stream, SNAPSHOT_DIR, overwrite=True, use_csv=True)

# Write the final output (a full GL Report)
gs.to_export(gl_report, stream, OUTPUT_DIR, export_format="csv")
