import os
import pandas as pd

def add_sitecode_column(base_path, save_path):
    reference_file = os.path.join(base_path, 'Kind-of-participant.csv')
    if not os.path.exists(reference_file):
        print("❌ Reference file 'Kind-of-participant.csv' not found.")
        return

    try:
        reference_df = pd.read_csv(reference_file, sep=';', encoding='utf-8')
    except Exception as e:
        print(f"❌ Failed to read reference file: {e}")
        return

    if 'participant_identifier' not in reference_df.columns or 'Site' not in reference_df.columns:
        print("❌ Reference file missing required columns.")
        print(f"Available columns: {reference_df.columns.tolist()}")
        return

    reference_map = reference_df.set_index('participant_identifier')['Site'].to_dict()
    log_entries = []
    missing_records = []

    if not os.path.exists(save_path):
        os.makedirs(save_path)

    excluded_files = {'study-participant-forms.csv', "participants.csv", "study-queries.csv"}
    csv_files = [f for f in os.listdir(base_path) if f.endswith(".csv") and f not in excluded_files]

    if not csv_files:
        print(f"❌️ No CSV files found in base path: {os.path.abspath(base_path)}")
        return

    for file_name in csv_files:
        file_path = os.path.join(base_path, file_name)
        # print(f">>> Processing file: {file_name}")
        try:
            df = pd.read_csv(file_path, sep=';', encoding='utf-8')
        except Exception as e:
            print(f"❌ Failed to read {file_name}: {e}")
            continue

        if 'participant_identifier' not in df.columns:
            print(f"❌ Skipped {file_name}: No 'participant_identifier' column.")
            continue

        site_codes = []
        not_found = []

        for pid in df['participant_identifier']:
            if pid in reference_map:
                site_codes.append(reference_map[pid])
            else:
                site_codes.append(None)
                not_found.append(pid)

        for pid in not_found:
            missing_records.append((file_name, pid))

        df['SiteCode'] = pd.Series(site_codes, dtype="Int64")

        if 'participant_identifier' in df.columns and 'SiteCode' in df.columns:
            cols = df.columns.tolist()
            pid_index = cols.index("participant_identifier")
            cols.insert(pid_index + 1, cols.pop(cols.index("SiteCode")))
            df = df[cols]

        # Convert float columns to Int64 only if all values are integer-like
        for col in df.columns:
            if pd.api.types.is_float_dtype(df[col]):
                if df[col].dropna().apply(float.is_integer).all():
                    df[col] = df[col].astype('Int64')

        output_path = os.path.join(save_path, file_name)
        try:
            df.to_csv(output_path, index=False, encoding='utf-8', sep=';')
            # print(f"✅ Saved updated file to: {os.path.abspath(output_path)}")
        except Exception as e:
            print(f"❌ Failed to save {file_name}: {e}")
            continue

        success_count = len(df) - len(not_found)
        fail_count = len(not_found)
        log_entries.append(f"{file_name} - SiteCode added: {success_count}, Missing: {fail_count}")

        if not_found:
            log_entries.append(f"{file_name} - Unmatched participant_identifiers: {sorted(set(not_found))}")
            print(f"❗ {file_name} - Missing participant_identifiers ({len(not_found)}): {sorted(set(not_found))}")


    if log_entries:
        log_file_path = os.path.join(save_path, '_sitecode_log.txt')
        with open(log_file_path, 'w', encoding='utf-8') as log_file:
            log_file.write('\n'.join(log_entries))
        # print(f"✅ SiteCode log file created at {os.path.abspath(log_file_path)}")


    if missing_records:
        missing_log_path = os.path.join(save_path, '_sitecode_missing.txt')
        with open(missing_log_path, 'w', encoding='utf-8') as f:
            f.write("file_name: participant_identifier\n")
            for file_name, pid in missing_records:
                f.write(f"{file_name}: {pid}\n")
        print(f"✅⚠️ Missing SiteCodes saved to: {os.path.abspath(missing_log_path)}")

    print(f"--- ✅✅✅ SiteCode processing completed! All outputs have been saved to: {save_path} ------------")

def add_visitcode_column(base_path, save_path):
    visit_map = {
        0: [
            "Screening", "Enrolment (patient)", "Enrolment (patient) CSRI", "Enrolment (Clinician)",
            "Baseline", "Baseline (patient)", "ESM Baseline",
            "Baseline (team lead)", "Baseline (finance staff)", "Baseline (clinician)"
        ],
        1: [
            "T1 (2 months)", "T1 (patient) CSRI", "T1 (2 months) (patient)",
            "ESM T1", "T1 (2 months) (clinician)", "T1 (2month) (2nd clinician)"
        ],
        2: [
            "T2 (6 months)", "T2 (patient) CSRI", "T2 (6 months) (patient)",
            "ESM T2", "T2 (6 months) (clinician)", "T2 (6month) (2nd clinician)"
        ],
        3: [
            "T3 (12 months)", "T3 (patient) CSRI", "T3 (12 months) (patient)",
            "ESM T3", "T3 (12 months) (clinician)", "T3 (12month) (2nd clinician)"
        ]
    }

    value_to_code = {v: code for code, values in visit_map.items() for v in values}

    if not os.path.exists(save_path):
        os.makedirs(save_path)

    log_entries = []
    missing_records = []

    # csv_files = [f for f in os.listdir(base_path) if f.endswith(".csv")]
    excluded_files = {'study-participant-forms.csv', "participants.csv", "study-queries.csv"}
    csv_files = [f for f in os.listdir(base_path) if f.endswith(".csv") and f not in excluded_files]

    for file_name in csv_files:
        file_path = os.path.join(base_path, file_name)
        # print(f">>> Processing file: {file_name}")
        try:
            df = pd.read_csv(file_path, sep=';', encoding='utf-8')
        except Exception as e:
            print(f"❌ Failed to read {file_name}: {e}")
            continue

        if "visit_name" not in df.columns:
            print(f"⚠️ Skipped {file_name}: No 'visit_name' column.")
            continue

        visit_codes = []
        for idx, val in df['visit_name'].items():
            if val in value_to_code:
                visit_codes.append(value_to_code[val])
            else:
                visit_codes.append(pd.NA)
                missing_records.append((file_name, val))

        df['VisitCode'] = pd.Series(visit_codes, dtype="Int64")

        if 'participant_identifier' in df.columns and 'VisitCode' in df.columns:
            cols = df.columns.tolist()
            v_idx = cols.index('participant_identifier')
            cols.insert(v_idx + 1, cols.pop(cols.index('VisitCode')))
            df = df[cols]

        success_count = df['VisitCode'].notna().sum()
        fail_count = df['VisitCode'].isna().sum()
        log_entries.append(f"{file_name} - VisitCode added: {success_count}, Missing: {fail_count}")

        # Convert float columns to Int64 only if all values are integer-like
        for col in df.columns:
            if pd.api.types.is_float_dtype(df[col]):
                if df[col].dropna().apply(float.is_integer).all():
                    df[col] = df[col].astype('Int64')

        output_path = os.path.join(save_path, file_name)
        try:
            df.to_csv(output_path, index=False, encoding='utf-8', sep=';')
            # print(f"✅ Saved with VisitCode to: {os.path.abspath(output_path)}")
        except Exception as e:
            print(f"❌ Failed to save {file_name}: {e}")

    # Save Logfiles
    if log_entries:
        log_file_path = os.path.join(save_path, '_visitcode_log.txt')
        with open(log_file_path, 'w', encoding='utf-8') as f:
            f.write('\n'.join(log_entries))
        # print(f"✅ VisitCode log file saved to: {os.path.abspath(log_file_path)}")

    # if missing_records:
    #     missing_log_path = os.path.join(save_path, 'visitcode_missing.txt')
    #     with open(missing_log_path, 'w', encoding='utf-8') as f:
    #         f.write("file_name,visit_name\n")
    #         for fname, visit in missing_records:
    #             f.write(f"{fname},{visit}\n")
    #     print(f"Missing VisitCodes saved to: {os.path.abspath(missing_log_path)}")

    if missing_records:
        missing_log_path = os.path.join(save_path, '_visitcode_missing.txt')
        with open(missing_log_path, 'w', encoding='utf-8') as f:
            f.write("file_name: visit_name\n")
            for fname, visit in missing_records:
                f.write(f"{fname}: {visit}\n")
        print(f"✅⚠️ Missing VisitCodes saved to: {os.path.abspath(missing_log_path)}")

    print(f"--- ✅✅✅ VisitCode processing completed! All outputs have been saved to: {save_path} ------------")


