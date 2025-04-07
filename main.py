import pandas as pd
import numpy as np
import os
import yaml
from datetime import datetime
import base_variables
import measure_calculation_woCopy
import id_processing
import merge_redcap_n_maganamed


# Read configuration file
with open("config.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

# Get paths from config
base_path_maganamed = config['maganamedPath']['base_path']
base_path_reference = config['maganamedPath']['base_path_reference']
base_path_redcap = os.path.join(config['idListsPath']['save_path'], "redcap_with_allocation.csv") # Config 추가

save_path_baseVar = config['maganamedPath']['save_path_baseVar']
save_path_calVar = config['maganamedPath']['save_path_calVar']
save_path_idProcessed = config['maganamedPath']['save_path_idProcessed']
save_path_redcapIntegrated = config['maganamedPath']['save_path_redcapIntegrated'] # Config 추가


# Call the processing function
# (1) add base variables (SiteCode, VisitCode)
base_variables.add_sitecode_column(base_path_maganamed, save_path_baseVar)
base_variables.add_visitcode_column(save_path_baseVar, save_path_baseVar)

# (2) add calculated values
measure_calculation_woCopy.calculate_and_save(save_path_baseVar, save_path_calVar)
measure_calculation_woCopy.copy_unprocessed_files(save_path_baseVar, save_path_calVar)

# (3) perform the id processing
dfs, delete_log, exchange_log, merge_log  = id_processing.run_id_processing_and_save(base_path_reference, save_path_calVar, save_path_idProcessed)

# (4) Integrate REDCap and filtering
df_redcapInfos = pd.read_csv(base_path_redcap, sep=';')
merge_redcap_n_maganamed.merge_redcap_n_maganamed(df_redcapInfos, save_path_idProcessed, save_path_redcapIntegrated)




# Optional: Use this to inspect the reference excel file after it has been updated
# required_visits = [
#     "Screening", "Enrolment (patient)", "Baseline (patient)",
#     "T1 (2 months) (patient)", "T2 (6 months) (patient)", "T3 (12 months) (patient)",
#     "Enrolment (patient) CSRI", "T1 (patient) CSRI", "T2 (patient) CSRI", "T3 (patient) CSRI",
#     "ESM Baseline", "ESM T1", "ESM T2", "ESM T3",
#     "Baseline", "T1 (2 months)", "T2 (6 months)", "T3 (12 months)",
#     "Enrolment (Clinician)", "Baseline (clinician)", "T1 (2 months) (clinician)",
#     "T2 (6 months) (clinician)", "T3 (12 months) (clinician)"
# ]
# df_ref = id_processing.load_reference_excel(base_path_reference)
# referLog_df = id_processing.check_missing_and_duplicate_visits_per_cid(df_ref, required_visits, output_path=os.path.join(save_path_idProcessed, "__check_refer_excel_visits_log.csv"))