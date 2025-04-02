import pandas as pd
import os

def load_reference_excel(refer_path):
    ref_file = os.path.join(refer_path, "table_for_IDprocessing_allCentersVer4.xlsx")
    df_ref = pd.read_excel(ref_file)

    df_ref.rename(columns={
        'Current ID': 'current_id',
        'Act1: \nDelete complete data?': 'act1_delete',
        'Act2: \nKeep complete data?': 'act2_keep',
        'Act3: \nMove data?': 'act3_move',
        'Act4: \nMerge data?\n(1: merge v, 2: merge c)': 'act4_merge',
        '\nAct3: MOVE\ninto which ID': 'act3_move_id',
        '\nAct3: MOVE\ninto which visit': 'act3_move_visit',
        '\nAct4: MERGE 0\nKeep data of this ID until..': 'act4_until',
        '\nAct4: MERGE 1\nwith which ID': 'act4_merge_id',
        '\nAct4: MERGE 1\ndata of other ID from..': 'act4_merge_visit',
        'ultimate ID': 'final_id',
        'Check': 'check'
    }, inplace=True)

    # print("실제 엑셀 컬럼명:", df_ref.columns.tolist())  # 디버깅용 출력

    expected_cols = [
        "current_id", "act1_delete", "act2_keep", "act3_move", "act4_merge", "act3_move_id", "act3_move_visit",
        "act4_until", "act4_merge_id", "act4_merge_visit", "final_id", "check"
    ]
    df_ref = df_ref[expected_cols]

    df_ref["act3_move_visit"] = df_ref["act3_move_visit"].fillna("").apply(lambda x: [i.strip() for i in x.split(",") if i.strip()])
    df_ref["act4_merge_visit"] = df_ref["act4_merge_visit"].fillna("").apply(lambda x: [i.strip() for i in x.split(",") if i.strip()])

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

    return dfs, delete_log

def apply_move_operations(df_ref, dfs, remember_original_data):
    move_log = []

    for _, row in df_ref.iterrows():
        cid = row["current_id"]
        act3 = str(row["act3_move"]).strip()

        if "1" in act3:
            target_id = row["act3_move_id"]
            target_visits = row["act3_move_visit"]  # expected to be a list like ["T1", "T2"]

            for file, df in dfs.items():
                if "participant_identifier" not in df.columns or "visit_name" not in df.columns:
                    continue

                for visit in target_visits:
                    # Source condition: current ID + visit
                    condition = (df["participant_identifier"] == cid) & (df["visit_name"] == visit)
                    rows_to_move = df[condition]

                    if rows_to_move.empty:
                        continue

                    # Target condition: destination ID + visit
                    target_condition = (df["participant_identifier"] == target_id) & (df["visit_name"] == visit)
                    original_target_data = df[target_condition]

                    # Store original target data (only once)
                    remember_key = (file, target_id, visit)
                    if remember_key not in remember_original_data and not original_target_data.empty:
                        remember_original_data[remember_key] = original_target_data.copy()

                    # Remove existing target data to avoid duplication
                    df = df[~target_condition]

                    # Replace ID in the copied rows
                    moved_rows = rows_to_move.copy()
                    moved_rows["participant_identifier"] = target_id

                    # Remove source data (actual move)
                    df = df[~condition]

                    # Append moved rows to DataFrame
                    dfs[file] = pd.concat([df, moved_rows], ignore_index=True)

                    # Log the move
                    move_log.append({
                        "filename": file,
                        "from_id": cid,
                        "to_id": target_id,
                        "visit_name": visit,
                        "rows_moved": len(moved_rows)
                    })

    return dfs, remember_original_data, move_log

