import os
import pandas as pd

def merge_redcap_n_maganamed(df_redcapInfos, maganamed_folder_path, save_path):
    """
    Adds 'unit', 'condition', and 'randomize' from REDCap info to all MaganaMed CSV files,
    and saves both full merged and condition-filtered versions.

    Args:
        df_redcapInfos (pd.DataFrame): REDCap data containing 'record_id', 'unit', 'condition', 'randomize'.
        maganamed_folder_path (str): Path to MaganaMed CSV files.
        save_path (str): Output folder path to save merged and filtered files.
    """
    os.makedirs(save_path, exist_ok=True)
    output_folder_path = os.path.join(save_path, '01_integrated')
    filtered_folder_path = os.path.join(save_path, '02_filtered')
    os.makedirs(output_folder_path, exist_ok=True)
    os.makedirs(filtered_folder_path, exist_ok=True)

    csv_files = [f for f in os.listdir(maganamed_folder_path) if f.endswith('.csv')]
    special_files = ["Demographics-(Clinicians).csv", "cliniciansAnswer1.csv", "cliniciansAnswer3.csv"]

    for csv_file in csv_files:
        file_path = os.path.join(maganamed_folder_path, csv_file)

        try:
            df_maganamed = pd.read_csv(file_path, sep=';')
            if df_maganamed.empty:
                print(f"Skipping {csv_file}: File is empty.")
                continue
        except Exception as e:
            print(f"❌ Error reading {file_path}: {e}")
            continue

        if 'participant_identifier' not in df_maganamed.columns:
            print(f"Skipping {csv_file}: No 'participant_identifier' column found.")
            continue

        merged_df = pd.merge(
            df_maganamed,
            df_redcapInfos[['study_id', 'unit', 'condition', 'randomize']],
            left_on='participant_identifier',
            right_on='study_id',
            how='left'
        )

        unmatched_rows = merged_df[merged_df['unit'].isna()]
        print(f"Unmatched rows in {csv_file}: {len(unmatched_rows)}")

        # Drop unnecessary columns
        merged_df.drop(columns=['record_id', 'study_id'], inplace=True, errors='ignore')

        # Insert new columns after 'center_name'
        if 'center_name' in merged_df.columns:
            center_index = merged_df.columns.get_loc('center_name') + 1
            cols = list(merged_df.columns)
            if 'unit' in cols: cols.remove('unit')
            if 'condition' in cols: cols.remove('condition')
            if 'randomize' in cols: cols.remove('randomize')
            cols.insert(center_index, 'unit')
            cols.insert(center_index + 1, 'condition')
            cols.insert(center_index + 2, 'randomize')

            merged_df = merged_df[cols]

        # For special clinician files, set fixed value
        if csv_file in special_files:
            for col in ['unit', 'condition', 'randomize']:
                if col in merged_df.columns:
                    merged_df[col] = 999

        # Convert float columns to Int64 to preserve NaN
        float_cols = merged_df.select_dtypes(include=['float']).columns
        for col in float_cols:
            merged_df[col] = pd.to_numeric(merged_df[col], errors='coerce')
            if (merged_df[col].dropna() % 1 == 0).all():
                merged_df[col] = merged_df[col].astype('Int64')
            else:
                # print(f" Skipped conversion to Int64 for '{col}' due to float values.")

        # Save merged file
        output_file_path = os.path.join(output_folder_path, csv_file)
        merged_df.to_csv(output_file_path, sep=';', index=False)
        print(f"✅ Saved merged: {output_file_path}")



        # Save filtered version (condition 1 or 999) #TODO: no needs more if we have research database
        if 'condition' in merged_df.columns:
            # filtered_df = merged_df[merged_df['condition'] == 1]
            filtered_df = merged_df[merged_df['condition'].isin([1, 999])]
            filtered_df = filtered_df.drop(columns=['condition'])  # 'condition' drop
            filtered_df = filtered_df.drop(columns=['randomize'])  # 'randomize' drop
            filtered_output_path = os.path.join(filtered_folder_path, csv_file)
            filtered_df.to_csv(filtered_output_path, sep=';', index=False)
            print(f"Filtered file saved: {filtered_output_path}")

    print("✅ All files processed and saved successfully.")
