import pandas as pd
import os

def load_reference_excel(refer_path):
    ref_file = os.path.join(refer_path, "table_for_IDprocessing_allCentersVer6.xlsx")
    df_ref = pd.read_excel(ref_file)

    df_ref.rename(columns={
        'Current ID': 'current_id',
        'Act1: \nDelete complete data?': 'act1_delete',
        'Act2: \nKeep complete data?': 'act2_keep',
        'Act3: \nExchange data?': 'act3_exchange',
        'Act4: \nMerge data?\n(1: merge v, 2: merge c)': 'act4_merge',
        '\nAct3: EXCHANGE\nwith which ID': 'act3_exchange_id',
        '\nAct3: EXCHANGE\nof which visit': 'act3_exchange_visit',
        '\nAct4: MERGE 0\nKeep data of this ID until..': 'act4_merge_until',
        '\nAct4: MERGE 0\nalso merge these visits': 'act4_also_visit',
        '\nAct4: MERGE 1\nwith which ID': 'act4_merge_id',
        '\nAct4: MERGE 1\ndata of other ID from..': 'act4_merge_visit',
        'ultimate ID': 'final_id',
        'Check': 'check'
    }, inplace=True)

    expected_cols = [
        "current_id", "act1_delete", "act2_keep", "act3_exchange", "act4_merge", "act3_exchange_id", "act3_exchange_visit",
        "act4_merge_until", "act4_also_visit", "act4_merge_id", "act4_merge_visit", "final_id", "check"
    ]
    df_ref = df_ref[expected_cols]

    df_ref["act3_exchange_visit"] = df_ref["act3_exchange_visit"].fillna("").apply(lambda x: [i.strip() for i in x.split(",") if i.strip()])
    df_ref["act4_merge_visit"] = df_ref["act4_merge_visit"].fillna("").apply(lambda x: [i.strip() for i in x.split(",") if i.strip()])
    df_ref["act4_merge_until"] = df_ref["act4_merge_until"].fillna("").apply(lambda x: [i.strip() for i in x.split(",") if i.strip()])
    df_ref["act4_also_visit"] = df_ref["act4_also_visit"].fillna("").apply(lambda x: [i.strip() for i in x.split(",") if i.strip()])
    # print("--- Loading xlsx file is complete. ---")
    return df_ref



def load_all_csvs(base_path):
    dfs = {}
    excluded_files = {"participants.csv", "study-queries.csv", "study-participant-forms.csv"}
    csv_files = [f for f in os.listdir(base_path) if f.endswith(".csv") and f not in excluded_files]
    for file in csv_files:
        path = os.path.join(base_path, file)
        try:
            df = pd.read_csv(path, sep=';', encoding='utf-8', on_bad_lines='skip', dtype='object')
            dfs[file] = df
        except Exception as e:
            print(f"❌ Error loading {file}: {e}")
    # print("--- Loading csv files is complete. ---")
    return dfs

def apply_delete_operations(df_ref, dfs):
    delete_log = []

    for _, row in df_ref.iterrows():
        cid = row["current_id"]
        act1 = str(row["act1_delete"]).strip()

        if "1" in act1:
            for file, df in dfs.items():
                if "participant_identifier" not in df.columns:
                    continue

                rows_to_delete = df[df["participant_identifier"] == cid]
                visit_names = (
                    rows_to_delete["visit_name"].unique().tolist()
                    if "visit_name" in rows_to_delete.columns else ["unknown"]
                )

                # Log
                for visit in visit_names:
                    delete_log.append({
                        "filename": file,
                        "deleted_id": cid,
                        "visit_name": visit
                    })
                # Apply deletion by keeping only rows that do NOT match the current ID
                dfs[file] = df[df["participant_identifier"] != cid]
            # print(f"--- Deleting process of {cid} is complete. ---")

    return dfs, delete_log


def apply_exchange_operations(df_ref, dfs):
    exchange_log = []

    for _, row in df_ref.iterrows():
        cid = row["current_id"]
        act3 = str(row["act3_exchange"]).strip()

        if "1" in act3:
            target_id = row["act3_exchange_id"]
            target_visits = row["act3_exchange_visit"]

            for file, df in dfs.items():
                if "participant_identifier" not in df.columns or "visit_name" not in df.columns:
                    continue

                for visit in [v.strip() for v in target_visits]:
                    # Create masks for current ID and target ID at the given visit
                    cid_mask = (df["participant_identifier"] == cid) & (df["visit_name"] == visit)
                    tid_mask = (df["participant_identifier"] == target_id) & (df["visit_name"] == visit)
                    # Extract the original data for both IDs
                    cid_rows = df[cid_mask].copy()
                    tid_rows = df[tid_mask].copy()
                    # (Optional) Swap participant_identifier (commented out)
                    # cid_rows["participant_identifier"] = target_id
                    # tid_rows["participant_identifier"] = cid
                    # Raise an error if the number of rows doesn't match
                    if len(cid_rows) != len(tid_rows):
                        raise ValueError(f"Row count mismatch between {cid} and {target_id} for visit {visit}")

                    # Define columns to swap (exclude ID, created_at, visit_name)
                    columns_to_swap = [col for col in df.columns if
                                       col not in ["participant_identifier", "created_at", "visit_name"]]
                    # Swap values between the two sets of rows
                    cid_rows[columns_to_swap], tid_rows[columns_to_swap] = (
                        tid_rows[columns_to_swap].values,
                        cid_rows[columns_to_swap].values
                    )
                    # Replace the original rows with the exchanged ones
                    df = df[~cid_mask & ~tid_mask]
                    df = pd.concat([df, cid_rows, tid_rows], ignore_index=True)
                    dfs[file] = df

                    # Save log info
                    exchange_log.append({
                        "filename": file,
                        "exchanged_id_1": cid,
                        "exchanged_id_2": target_id,
                        "visit_name": visit,
                        "rows_exchanged_from_1": len(cid_rows),
                        "rows_exchanged_from_2": len(tid_rows)
                    })
            # print(f"--- Exchanging process btw {cid} and {target_id} is complete. ---")

    return dfs, exchange_log


def apply_merge_operations(df_ref, dfs):
    """
    For each row in df_ref (with current_id, merge_id, visits, etc.),
    perform merging across all files in dfs based on the following logic:

    - If at least one visit in act4_merge_visit exists in the data, merge based on those visits.
    - If none exist, fall back to act4_merge_until + act4_also_visit and follow this logic:

      (1) For each visit, check whether rows exist for cid and/or mid.
      (2) Compare values from the 'diary_date' column up to the 5th column from the end:
          - If all mid values are empty → keep cid
          - If values are equal → keep cid
          - If values are different → log as conflict
    """
    merge_log = []

    for _, row in df_ref.iterrows():
        cid = row["current_id"]
        act4 = str(row["act4_merge"]).strip()

        if "1" in act4 or "2" in act4:
            mid = row["act4_merge_id"]
            visits_merge = row["act4_merge_visit"]
            visits_until = row["act4_merge_until"]
            visits_also = row["act4_also_visit"]
            final_id = row["final_id"]

            for file, df in dfs.items():
                if "participant_identifier" not in df.columns or "visit_name" not in df.columns:
                    continue

                existing_visits = set(visits_merge).intersection(df["visit_name"].unique()) if isinstance(visits_merge, list) else set()

                if existing_visits:
                    # Merge based on act4_merge_visit
                    common_visits = list(existing_visits)
                    mask_cid = (df["participant_identifier"] == cid) & (df["visit_name"].isin(common_visits))
                    df = df[~mask_cid]

                    mask_mid = (df["participant_identifier"] == mid) & (df["visit_name"].isin(common_visits))
                    rows_to_move = df[mask_mid].copy()
                    rows_moved_count = len(rows_to_move)
                    rows_to_move["participant_identifier"] = cid

                    df = df[df["participant_identifier"] != mid]
                    rows_to_move["participant_identifier"] = final_id
                    df.loc[df["participant_identifier"] == cid, "participant_identifier"] = final_id

                    df = pd.concat([df, rows_to_move], ignore_index=True)
                    df = df.sort_values(by=["participant_identifier"]).reset_index(drop=True)
                    dfs[file] = df

                    merge_log.append({
                        "filename": file,
                        "current_id": cid,
                        "merge_id": mid,
                        "final_id": final_id,
                        "visits_merged": visits_merge,
                        "common_visits": common_visits,
                        "rows_moved": rows_moved_count,
                        "note": "merged by act4_merge_visit"
                    })

                else:
                    # Merge based on act4_merge_until + act4_also_visit (i.e., process all other visits)
                    until_visits = []
                    if isinstance(visits_until, list):
                        until_visits.extend(visits_until)
                    if isinstance(visits_also, list):
                        until_visits.extend(visits_also)
                    until_visits = [v.strip() for v in until_visits if v.strip()]

                    diary_col_idx = df.columns.get_loc("diary_date") + 1
                    data_cols = df.columns[diary_col_idx:6]  # 6 columns excluded, because of calculated values

                    for visit in until_visits:
                        cid_mask = (df["participant_identifier"] == cid) & (df["visit_name"] == visit)
                        mid_mask = (df["participant_identifier"] == mid) & (df["visit_name"] == visit)

                        if not cid_mask.any() and not mid_mask.any():
                            continue

                        cid_row = df[cid_mask]
                        mid_row = df[mid_mask]

                        if cid_row.empty and not mid_row.empty:
                            df.loc[mid_mask, "participant_identifier"] = cid
                            df.loc[df["participant_identifier"] == cid, "participant_identifier"] = final_id
                            merge_log.append({
                                "filename": file, "visit": visit, "cid": cid, "mid": mid,
                                "final_id": final_id, "action": "mid only → moved to cid (act4_until)"
                            })

                        elif not cid_row.empty and mid_row.empty:
                            df = df[~mid_mask]
                            df.loc[df["participant_identifier"] == cid, "participant_identifier"] = final_id
                            merge_log.append({
                                "filename": file, "visit": visit, "cid": cid, "mid": mid,
                                "final_id": final_id, "action": "cid only → kept (act4_until)"
                            })

                        elif not cid_row.empty and not mid_row.empty:
                            cid_vals = cid_row[data_cols].values
                            mid_vals = mid_row[data_cols].values

                            cid_data = ["" if pd.isna(x) else str(x).strip() for x in cid_vals[0]]
                            mid_data = ["" if pd.isna(x) else str(x).strip() for x in mid_vals[0]]

                            if all(x == "" for x in mid_data):
                                df = df[~mid_mask]
                                df.loc[df["participant_identifier"] == cid, "participant_identifier"] = final_id
                                merge_log.append({
                                    "filename": file, "visit": visit, "cid": cid, "mid": mid,
                                    "final_id": final_id, "action": "mid empty → kept cid (act4_until)"
                                })

                            elif cid_data == mid_data:
                                df = df[~mid_mask]
                                df.loc[df["participant_identifier"] == cid, "participant_identifier"] = final_id
                                merge_log.append({
                                    "filename": file, "visit": visit, "cid": cid, "mid": mid,
                                    "final_id": final_id, "action": "both same → kept cid (act4_until)"
                                })

                            else:
                                print("📌 비교 컬럼:", list(data_cols))
                                print("CID data:", cid_data)
                                print("MID data:", mid_data)
                                merge_log.append({
                                    "filename": file, "visit": visit, "cid": cid, "mid": mid,
                                    "final_id": final_id, "action": "conflict → manual check needed"
                                })
                                print(f"[CONFLICT] {file} - {cid} vs {mid} @ {visit}: data mismatch")

                    df = df.sort_values(by=["participant_identifier"]).reset_index(drop=True)
                    dfs[file] = df
            print(f"--- Merging process btw {cid} and {mid} is complete. ---")

    return dfs, merge_log


def run_id_processing_and_save(refer_path, base_path, save_path):
    """
    Process deletion and move operations for participant IDs,
    and save updated CSV files and logs to save_path.

    Args:
        refer_path (str): Path to reference Excel file.
        base_path (str): Path to input CSV files.
        save_path (str): Path to save updated CSVs and logs.

    Returns:
        dfs (dict): Updated DataFrames.
        delete_log (list): Deletion records.
        move_log (list): Movement records.
    """


    # 1. Load reference table and CSV files
    df_ref = load_reference_excel(refer_path)
    dfs = load_all_csvs(base_path)
    remember_original_data = {}

    # 2. Apply deletion
    dfs, delete_log = apply_delete_operations(df_ref, dfs)

    # 3. Apply exchang
    dfs, exchange_log = apply_exchange_operations(df_ref, dfs)

    # 4. Apply
    dfs, merge_log = apply_merge_operations(df_ref, dfs)

    # 4. Save updated CSVs to save_path
    os.makedirs(save_path, exist_ok=True)
    for filename, df in dfs.items():
        out_path = os.path.join(save_path, filename)
        df.to_csv(out_path, sep=';', index=False, encoding='utf-8')

    # 5. Save logs
    pd.DataFrame(delete_log).to_csv(os.path.join(save_path, "_delete_log.csv"), sep=';', index=False, encoding='utf-8-sig')
    pd.DataFrame(exchange_log).to_csv(os.path.join(save_path, "_exchange_log.csv"), sep=';', index=False, encoding='utf-8-sig')
    pd.DataFrame(merge_log).to_csv(os.path.join(save_path, "_merge_log.csv"), sep=';', index=False, encoding='utf-8-sig')


    return dfs, delete_log, exchange_log, merge_log



def check_missing_and_duplicate_visits_per_cid(df_ref, required_visits, output_path=None):
    """
    For each current_id in df_ref, check if the union of visits in:
    - act4_merge_until
    - act4_also_visit
    - act4_merge_visit

    contains all visits in the required_visits list.

    Also checks for duplicated visits across the combined list.
    Optionally saves the result to a CSV if output_path is given.
    """
    visit_check_log = []

    for _, row in df_ref.iterrows():
        cid = row["current_id"]
        act4 = str(row.get("act4_merge", "")).strip()
        print(cid)

        # Use loose substring matching as requested
        if "1" not in act4 and "2" not in act4:
            continue

        visits_until = row.get("act4_merge_until", [])
        visits_also = row.get("act4_also_visit", [])
        visits_merge = row.get("act4_merge_visit", [])

        # Combine all visit lists
        combined_visits = visits_until + visits_also + visits_merge
        all_visits_set = set(combined_visits)

        # Check missing visits
        missing_visits = sorted(set(required_visits) - all_visits_set)

        # Check duplicated visits
        visit_counts = {}
        for v in combined_visits:
            visit_counts[v] = visit_counts.get(v, 0) + 1
        duplicated_visits = [v for v, count in visit_counts.items() if count > 1]

        # Log if any issues
        if missing_visits or duplicated_visits:
            visit_check_log.append({
                "current_id": cid,
                "missing_count": len(missing_visits),
                "missing_visits": ", ".join(missing_visits),
                "duplicated_visits": ", ".join(duplicated_visits)
            })

    log_df = pd.DataFrame(visit_check_log)

    if output_path:
        log_df.to_csv(output_path, sep=';', index=False)
        print(f"✅ Log saved to {output_path}")

    return log_df
