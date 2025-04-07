import pandas as pd
import numpy as np
import os
from datetime import datetime

def calculate_and_save(base_path, save_path):
    """
    Processes all files in the specified base path and saves the processed results to the save path.
    """
    # Get the current timestamp
    timestamp = datetime.now().strftime('%Y%m%d_%H-%M-%S')

    # File mappings
    file_mapping = {
        'Service-Attachement-Questionnaire-(SAQ).csv': 'df_SAQ',
        'Service-Engagement-Scale-(Clinician-rating).csv': 'df_SES',
        'Questionnaire-on-Process-of-Recovery-(QPR).csv': 'df_QPR',
        'Mental-Health-self-management-questionnaire-(MHSEQ).csv': 'df_MHSEQ',
        'SDMQ-(Patient-rating).csv': 'df_SDMQ_P',
        'SDMQ-(Clinician-rating).csv': 'df_SDMQ_C',
        'Goal-Attainment-Scale.csv': 'df_GAS',
        'Social-Functioning-Scale.csv': 'df_SFS',
        'Clinical-Global-Impression.csv': 'df_CGI',
        'General-Health-Questionnaire-(GHQ).csv': 'df_GHQ',
        'UCLA-Loneliness-Scale.csv': 'df_UCLA',
        'MANSA.csv': 'df_MANSA',
        'Demographics-(Clinicians).csv': 'df_DEMO_C',
        'Demographics-(Patients).csv': 'df_DEMO_P',
        'Self-injurious-Behavior-(T0).csv': 'df_SITBI_T0',
        'Brief-Experiential-Avoidance-Questionnaire-(BEAQ).csv': 'df_BEAQ',
        'Childhood-Trauma-Questionnaire-(CTQ).csv': 'df_CTQ',
        'Working-Alliance-(Clinician-rating).csv': 'df_WAI_C',
        'Working-Alliance-(Patient-rating).csv': 'df_WAI_P',
        'Reflective-Functioning.csv': 'df_RFQ',
        'Emotion-Regulation.csv': 'df_DERS'
    }

    # Ensure the save directory exists
    if not os.path.exists(save_path):
        os.makedirs(save_path)

    for file_name, df_name in file_mapping.items():
        file_path = os.path.join(base_path, file_name)

        try:
            # Load file
            df = pd.read_csv(file_path, sep=';')

            # Processing based on file type
            if df_name == 'df_SAQ':
                # SAQ processing
                df[['SAQ_01', 'SAQ_02', 'SAQ_03', 'SAQ_04', 'SAQ_05', 'SAQ_06', 'SAQ_07',
                    'SAQ_08', 'SAQ_09', 'SAQ_10', 'SAQ_11', 'SAQ_12', 'SAQ_13', 'SAQ_14',
                    'SAQ_15', 'SAQ_16', 'SAQ_17', 'SAQ_18', 'SAQ_19', 'SAQ_20', 'SAQ_21',
                    'SAQ_22', 'SAQ_23', 'SAQ_24', 'SAQ_25']] = df[['SAQ_01', 'SAQ_02', 'SAQ_03',
                                                                    'SAQ_04', 'SAQ_05', 'SAQ_06', 'SAQ_07', 'SAQ_08', 'SAQ_09', 'SAQ_10',
                                                                    'SAQ_11', 'SAQ_12', 'SAQ_13', 'SAQ_14', 'SAQ_15', 'SAQ_16', 'SAQ_17',
                                                                    'SAQ_18', 'SAQ_19', 'SAQ_20', 'SAQ_21', 'SAQ_22', 'SAQ_23', 'SAQ_24',
                                                                    'SAQ_25']].apply(pd.to_numeric, errors='coerce').astype('Int64')
                df['SAQ_total'] = df[['SAQ_01', 'SAQ_02', 'SAQ_03', 'SAQ_04', 'SAQ_05', 'SAQ_06', 'SAQ_07', 'SAQ_08', 'SAQ_09', 'SAQ_10',
                                      'SAQ_11', 'SAQ_12', 'SAQ_13', 'SAQ_14', 'SAQ_15', 'SAQ_16', 'SAQ_17', 'SAQ_18', 'SAQ_19', 'SAQ_20',
                                      'SAQ_21', 'SAQ_22', 'SAQ_23', 'SAQ_24', 'SAQ_25']].sum(axis=1)

            elif df_name == 'df_SES':
                # SES processing
                df[[f'SES_C_{i:02d}' for i in range(1, 15)]] = df[[f'SES_C_{i:02d}' for i in range(1, 15)]].apply(pd.to_numeric, errors='coerce').astype('Int64')
                df['SES_C_total'] = df[[f'SES_C_{i:02d}' for i in range(1, 15)]].sum(axis=1)

            elif df_name == 'df_QPR':
                # QPR processing
                df[[f'QPR_{i:02d}' for i in range(1, 16)]] = df[[f'QPR_{i:02d}' for i in range(1, 16)]].apply(pd.to_numeric, errors='coerce').astype('Int64')
                df['QPR_total'] = df[[f'QPR_{i:02d}' for i in range(1, 16)]].sum(axis=1)

            elif df_name == 'df_MHSEQ':
                # MHSEQ processing
                df[[f'MHSEQ_{i:02d}' for i in range(1, 19)]] = df[
                    [f'MHSEQ_{i:02d}' for i in range(1, 19)]].apply(pd.to_numeric, errors='coerce').astype('Int64')
                df['MHSEQ_total'] = df[[f'MHSEQ_{i:02d}' for i in range(1, 19)]].sum(axis=1)

            elif df_name == 'df_SDMQ_P':
                # SDMQ_P processing
                df[[f'SMDQ_P_{i:02d}' for i in range(1, 10)]] = df[
                    [f'SMDQ_P_{i:02d}' for i in range(1, 10)]].apply(pd.to_numeric, errors='coerce').astype('Int64')
                df['SMDQ_P_total'] = df[[f'SMDQ_P_{i:02d}' for i in range(1, 10)]].sum(axis=1)

            elif df_name == 'df_SDMQ_C':
                # SDMQ_C processing
                df[[f'SMDQ_C_{i:02d}' for i in range(1, 10)]] = df[
                    [f'SMDQ_C_{i:02d}' for i in range(1, 10)]].apply(pd.to_numeric, errors='coerce').astype('Int64')
                df['SMDQ_C_total'] = df[[f'SMDQ_C_{i:02d}' for i in range(1, 10)]].sum(axis=1)


            # elif df_name == 'df_GAS':
            #     # GAS processing

            elif df_name == 'df_SFS':
                # SFS processing
                df[[f'SFS_{i:02d}' for i in range(1, 11)]] = df[[f'SFS_{i:02d}' for i in range(1, 11)]].apply(
                    pd.to_numeric, errors='coerce').astype('Int64')
                df['SFS_total'] = df[[f'SFS_{i:02d}' for i in range(1, 11)]].sum(axis=1)

            elif df_name == 'df_CGI':
                # CGI processing
                # df = df.rename(columns={"CGI_01_Severity-of-illness": "cgi"})
                df['CGI_cgi'] = df['CGI_01_Severity-of-illness']

            elif df_name == 'df_GHQ':
                # GHQ processing
                df[[f'GHQ_{i:02d}' for i in range(1, 13)]] = df[[f'GHQ_{i:02d}' for i in range(1, 13)]].apply(
                    pd.to_numeric, errors='coerce').astype('Int64')
                df['GHQ_total'] = df[[f'GHQ_{i:02d}' for i in range(1, 13)]].sum(axis=1)

            elif df_name == 'df_UCLA':
                # UCLA processing
                df[[f'UCL_{i:02d}' for i in range(1, 4)]] = df[[f'UCL_{i:02d}' for i in range(1, 4)]].apply(
                    pd.to_numeric, errors='coerce').astype('Int64')
                df['UCL_total'] = df[[f'UCL_{i:02d}' for i in range(1, 4)]].sum(axis=1)

            elif df_name == 'df_MANSA':
                # MANSA processing
                df[[f'MANSA_{i:02d}' for i in range(1, 17)]] = df[
                    [f'MANSA_{i:02d}' for i in range(1, 17)]].apply(pd.to_numeric, errors='coerce').astype('Int64')
                df['MANSA_total'] = df[[f'MANSA_{i:02d}' for i in range(1, 17)]].sum(axis=1)
                df['MANSA_mean'] = (df['MANSA_total'] / 12).round(2)

            elif df_name == 'df_DEMO_C':
                # DEMO_C processing
                ## Clinician:
                ### Collect row names of rows where each Psychotherapy item is 1 and add Psychotherapy_other if it has a value
                def assign_DemoC_psychotherapy_school(row):
                    therapies = []
                    if row['Psychotherapy_CBT'] == 1:
                        therapies.append('CBT')
                    if row['Psychotherapy_DBT'] == 1:
                        therapies.append('DBT')
                    if row['Psychotherapy_ACT'] == 1:
                        therapies.append('ACT')
                    if row['Psychotherapy_PD'] == 1:
                        therapies.append('PD')
                    if row['Psychotherapy_ST'] == 1:
                        therapies.append('ST')
                    if row['Psychotherapy_EMDR'] == 1:
                        therapies.append('EMDR')
                    # Add if 'Psychotherapy _other' has a value
                    if 'Psychotherapy _other' in row and pd.notna(row['Psychotherapy _other']):
                        therapies.append(str(row['Psychotherapy _other']))  # Convert value to string and add if present
                    # Join list into a single string
                    return ', '.join(therapies)
                ### Add values to the 'PsychoThSchool' column
                df['DEMO_C_psychoThSchool'] = df.apply(assign_DemoC_psychotherapy_school, axis=1)
                ### Convert numeric data to Int64 after all data processing is complete, while keeping non-numeric text values unchanged
                numeric_columns = df.select_dtypes(include=['float64', 'int64']).columns
                df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, downcast='integer').astype('Int64')


            elif df_name == 'df_SITBI_T0':
                # SITTBI_T0 processing
                df['SI_days'] = df['SITBI-T0_02']
                df['NSSI_days'] = df['SITBI-T0_09']

            elif df_name == 'df_BEAQ':
                # BEAQ processing
                df[[f'BEAQ_{i:02d}' for i in range(1, 16)]] = df[
                    [f'BEAQ_{i:02d}' for i in range(1, 16)]].apply(pd.to_numeric, errors='coerce').astype('Int64')
                df['BEAQ_total'] = df[[f'BEAQ_{i:02d}' for i in range(1, 16)]].sum(axis=1)

            elif df_name == 'df_CTQ':
                # CTQ processing
                ## Convert to numeric
                df[[f'{i}_CTQ' for i in range(1, 29)]] = df[
                    [f'{i}_CTQ' for i in range(1, 29)]].apply(pd.to_numeric, errors='coerce').astype('Int64')
                df['CTQ_00z'] = df['CTQ_00z'].apply(pd.to_numeric, errors='coerce').astype('Int64')
                ## Transform reverse-coded items
                reverse_coded_items = ['2_CTQ', '5_CTQ', '7_CTQ', '13_CTQ', '19_CTQ', '26_CTQ', '28_CTQ']

                original_ctq_values = df[reverse_coded_items].values
                df[reverse_coded_items] = 6 - df[reverse_coded_items]
                ## Calculate the total of all items
                # df['CTQ_total_check'] = df[[f'{i}_CTQ' for i in range(1, 29)]].sum(axis=1)
                # CTQ_total 계산 (row sum)
                columns_to_sum = ['1_CTQ', '2_CTQ', '3_CTQ', '4_CTQ', '5_CTQ', '6_CTQ', '7_CTQ', '8_CTQ', '9_CTQ',
                                  '11_CTQ', '12_CTQ', '13_CTQ', '14_CTQ', '15_CTQ', '17_CTQ', '18_CTQ', '19_CTQ',
                                  '20_CTQ', '21_CTQ', '23_CTQ', '24_CTQ', '25_CTQ', '26_CTQ', '27_CTQ', '28_CTQ']

                df['CTQ_total'] = df[columns_to_sum].sum(axis=1)
                df[reverse_coded_items] = original_ctq_values


            elif df_name == 'df_WAI_P':
                # WAI_P processing
                df[[f'WAI-P_{i:02d}' for i in range(1, 13)]] = df[
                    [f'WAI-P_{i:02d}' for i in range(1, 13)]].apply(pd.to_numeric, errors='coerce').astype('Int64')
                df['WAI-P_total'] = df[[f'WAI-P_{i:02d}' for i in range(1, 13)]].sum(axis=1)

            elif df_name == 'df_WAI_C':
                # WAI_C processing
                df[[f'WAI-C_{i:02d}' for i in range(1, 13)]] = df[
                    [f'WAI-C_{i:02d}' for i in range(1, 13)]].apply(pd.to_numeric, errors='coerce').astype('Int64')
                df['WAI-C_total'] = df[[f'WAI-C_{i:02d}' for i in range(1, 13)]].sum(axis=1)

            elif df_name == 'df_RFQ':
                # RFQ processing
                df[[f'1_RFQ_{i:02d}' for i in range(1, 9)]] = df[
                    [f'1_RFQ_{i:02d}' for i in range(1, 9)]].apply(pd.to_numeric, errors='coerce').astype('Int64')
                ## 1. Recode process
                # Recoding with proper NaN handling
                df[['RFQ_c1', 'RFQ_c2', 'RFQ_c3', 'RFQ_c4', 'RFQ_c5', 'RFQ_c6']] = df[
                    ['1_RFQ_01', '1_RFQ_02', '1_RFQ_03', '1_RFQ_04', '1_RFQ_05', '1_RFQ_06']
                ].applymap(
                    lambda x: (
                        3 if pd.notna(x) and x == 1 else
                        2 if pd.notna(x) and x == 2 else
                        1 if pd.notna(x) and x == 3 else
                        0 if pd.notna(x) and x in [4, 5, 6, 7] else pd.NA
                    )
                ).astype('Int64')

                df[['RFQ_u2', 'RFQ_u4', 'RFQ_u5', 'RFQ_u6', 'RFQ_u8']] = df[
                    ['1_RFQ_02', '1_RFQ_04', '1_RFQ_05', '1_RFQ_06', '1_RFQ_08']
                ].applymap(
                    lambda x: (
                        3 if pd.notna(x) and x == 7 else
                        2 if pd.notna(x) and x == 6 else
                        1 if pd.notna(x) and x == 5 else
                        0 if pd.notna(x) and x in [1, 2, 3, 4] else pd.NA
                    )
                ).astype('Int64')

                df['RFQ_u7'] = df['1_RFQ_07'].apply(
                    lambda x: (
                        3 if pd.notna(x) and x == 1 else
                        2 if pd.notna(x) and x == 2 else
                        1 if pd.notna(x) and x == 3 else
                        0 if pd.notna(x) and x in [4, 5, 6, 7] else pd.NA
                    )
                ).astype('Int64')

                # 2. Calculate the average of RFQ_c and RFQ_u
                df['RFQ_c_submean'] = df[['RFQ_c1', 'RFQ_c2', 'RFQ_c3', 'RFQ_c4', 'RFQ_c5', 'RFQ_c6']].astype(
                    float).mean(
                    axis=1, skipna=True
                ).round(2)
                df['RFQ_u_submean'] = df[['RFQ_u2', 'RFQ_u4', 'RFQ_u5', 'RFQ_u6', 'RFQ_u7', 'RFQ_u8']].astype(
                    float).mean(
                    axis=1, skipna=True
                ).round(2)


            elif df_name == 'df_DERS':
                # DERS processing
                # 지울것 df = df.applymap(lambda x: pd.to_numeric(x, errors='coerce')).astype('Int64')
                df[[f'DERS_{i:02d}' for i in range(1, 17)]] = df[
                    [f'DERS_{i:02d}' for i in range(1, 17)]].apply(pd.to_numeric, errors='coerce').astype('Int64')
                # df = df.applymap(lambda x: pd.to_numeric(x, errors='coerce')).astype('Int64')
                # df['DERS_total'] = df[[f'DERS_{i:02d}' for i in range(1, 17)]].sum(axis=1)
                df['DERS_total'] = df[['DERS_01', 'DERS_02', 'DERS_03', 'DERS_04',
                                                 'DERS_05', 'DERS_06', 'DERS_07', 'DERS_08',
                                                 'DERS_09', 'DERS_10', 'DERS_11', 'DERS_12',
                                                 'DERS_13', 'DERS_14', 'DERS_15', 'DERS_16']].sum(axis=1)
                ## Subscores per dimension
                #### C = CLARITY (Lack of Emotional Clarity) subtotal calculation
                df['DERS_c_subtotal'] = df[['DERS_01', 'DERS_02']].sum(axis=1)
                #### G = GOALS (Difficulties Engaging in Goal-Directed Behaviour)
                df['DERS_g_subtotal'] = df[['DERS_03', 'DERS_07', 'DERS_15']].sum(axis=1)
                #### M = IMPULSE (Impulse Control Difficulties)
                df['DERS_m_subtotal'] = df[['DERS_04', 'DERS_08', 'DERS_11']].sum(axis=1)
                #### S = STRATEGIES (Limited Access to Effective Emotion Regulation Strategies)
                df['DERS_s_subtotal'] = df[['DERS_05', 'DERS_06', 'DERS_12', 'DERS_14']].sum(axis=1)
                #### N = NON-ACCEPTANCE (nonacceptance of Emotional Responses)
                df['DERS_n_subtotal'] = df[['DERS_09', 'DERS_10', 'DERS_13']].sum(axis=1)

            # Convert float columns to Int64 only if all values are integer-like
            for col in df.columns:
                if pd.api.types.is_float_dtype(df[col]):
                    if df[col].dropna().apply(float.is_integer).all():
                        df[col] = df[col].astype('Int64')

            # Save the processed dataframe
            save_file_path = os.path.join(save_path, file_name)
            df.to_csv(save_file_path, index=False, sep=';')
            # print(f"✅  Saved {df_name} to {save_file_path}")

        except Exception as e:
            print(f"Error processing {file_name}: {e}")

    print(f"--- ✅✅✅ Calculated Values processing completed! All outputs have been saved to: {save_path} ------------")


def copy_unprocessed_files(source_dir, target_dir):
    """
    Copy files from source_dir to target_dir only if they do not already exist in target_dir.
    Existing files in target_dir will not be overwritten.
    """
    copied_files = []

    if not os.path.exists(target_dir):
        os.makedirs(target_dir)

    for filename in os.listdir(source_dir):
        source_file = os.path.join(source_dir, filename)
        target_file = os.path.join(target_dir, filename)

        if not os.path.isfile(source_file):
            continue  # Skip directories or non-files

        if not os.path.exists(target_file):
            shutil.copy2(source_file, target_file)
            copied_files.append(filename)

    print(f"✅ {len(copied_files)} files copied (existing files were skipped)")
    return copied_files
