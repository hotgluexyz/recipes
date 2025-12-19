import pandas as pd
import gluestick as gs
import os
import json

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
stream = "GeneralLedgerCashReport"
gl_report = input.get(stream)


# Get the snapped report data
gl_report_snap = gs.read_snapshots(stream, SNAPSHOT_DIR)

# If we have a pre-existing snapshot and new incremental data, we'll need to combine them together
if gl_report_snap is not None and gl_report is not None and not gl_report.empty: # assuming it's an incremental sync
    # Mark any "Beginning Balance" records in the new incremental data with Memo = "Beginning Balance"
    gl_report.loc[gl_report["Date"] == "Beginning Balance", "Memo"] = "Beginning Balance"
    
    # Separate "Beginning Balance" records from regular transaction records in both datasets
    gl_report_snap_copy = gl_report_snap.copy()
    snapshot_bb_records = gl_report_snap_copy[gl_report_snap_copy["Memo"] == "Beginning Balance"]
    snapshot_regular_records = gl_report_snap_copy[gl_report_snap_copy["Memo"] != "Beginning Balance"]
    
    incremental_bb_records = gl_report[gl_report["Memo"] == "Beginning Balance"].copy()
    incremental_regular_records = gl_report[gl_report["Memo"] != "Beginning Balance"].copy()
    
    # Convert Date column to datetime for comparison (tz-naive)
    incremental_regular_records["Date"] = pd.to_datetime(incremental_regular_records["Date"], errors="coerce", utc=True)
    
    min_date_from_new_sync = incremental_regular_records["Date"].min()

    # For regular records, filter out the sliding window period (data that the incremental sync just brought in)
    snapshot_regular_records = snapshot_regular_records.copy()
    snapshot_regular_records["Date"] = pd.to_datetime(snapshot_regular_records["Date"], errors="coerce", utc=True)
    snapshot_regular_filtered = snapshot_regular_records[snapshot_regular_records["Date"] < min_date_from_new_sync]

    # Convert dates back to string format before concatenation to avoid tz-aware/tz-naive mixing
    snapshot_regular_filtered = snapshot_regular_filtered.copy()
    snapshot_regular_filtered["Date"] = snapshot_regular_filtered["Date"].dt.strftime("%Y-%m-%d")
    incremental_regular_records = incremental_regular_records.copy()
    incremental_regular_records["Date"] = incremental_regular_records["Date"].dt.strftime("%Y-%m-%d")

    # Combine all "Beginning Balance" records from both sources, then deduplicate keeping the oldest per AccountId
    all_bb_records = pd.concat([snapshot_bb_records, incremental_bb_records], ignore_index=True)
    if not all_bb_records.empty:
        all_bb_records = all_bb_records.drop_duplicates(subset=["AccountId"], keep="first")

    # Combine: deduplicated Beginning Balance + snapshot records before cutoff + new incremental regular data
    gl_report = pd.concat([all_bb_records, snapshot_regular_filtered, incremental_regular_records], ignore_index=True)
    
    # Handle any "Beginning Balance" records that still have "Beginning Balance" as Date (new accounts from incremental)
    # These need their dates calculated based on the account's minimum transaction date
    accounts_with_unprocessed_bb = gl_report[gl_report["Date"] == "Beginning Balance"]["AccountId"].unique()
    
    for account_id in accounts_with_unprocessed_bb:
        # Get all transactions for this account (excluding Beginning Balance)
        account_transactions = gl_report[
            (gl_report["AccountId"] == account_id) & 
            (gl_report["Date"] != "Beginning Balance")
        ]
        
        if not account_transactions.empty:
            # Get the minimum date for this account
            min_date = pd.to_datetime(account_transactions["Date"], errors="coerce").min()
            if pd.notna(min_date):
                # Calculate: min_date -> (LOOKBACK_PERIOD + 1) months ago -> last day of prior month
                look_back_period_months_ago = min_date - relativedelta(months=LOOKBACK_PERIOD + 1)
                beginning_balance_date = (look_back_period_months_ago.replace(day=1) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
                
                # Update the Beginning Balance records for this account
                mask = (gl_report["AccountId"] == account_id) & (gl_report["Date"] == "Beginning Balance")
                gl_report.loc[mask, "Date"] = beginning_balance_date
    
    # Save the combined snapshot
    gs.snapshot_records(gl_report, stream, SNAPSHOT_DIR, overwrite=True, use_csv=True)
else: # assuming it's a full sync
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
        # Calculate: min_date -> (LOOKBACK_PERIOD + 1) months ago -> last day of prior month
        look_back_period_months_ago = pd.to_datetime(min_date) - relativedelta(months=LOOKBACK_PERIOD + 1)
        beginning_balance_date = (look_back_period_months_ago.replace(day=1) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")

        if beginning_balance_date is not None:
            # Update the Beginning Balance records for this account
            mask = (gl_report["AccountId"] == account_id) & (gl_report["Date"] == "Beginning Balance")
            gl_report.loc[mask, "Memo"] = "Beginning Balance"
            gl_report.loc[mask, "Date"] = beginning_balance_date
            
    gl_report['Date'] = gl_report['Date'].astype("string")

    gs.snapshot_records(gl_report, stream, SNAPSHOT_DIR, overwrite=True, use_csv=True)

# Write the final output (a full GL Report)
gs.to_export(gl_report, stream, OUTPUT_DIR, export_format="csv")
