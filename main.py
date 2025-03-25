import pandas as pd
import numpy as np
import os
import yaml
from datetime import datetime
import base_variables
import measure_calculation_woCopy


# Read configuration file
with open("config.yaml", "r", encoding="utf-8") as f:
    config = yaml.safe_load(f)

# Get paths from config
base_path_maganamed = config['maganamedPath']['base_path']
save_path_baseVar = config['maganamedPath']['save_path_baseVar']
save_path_calVar = config['maganamedPath']['save_path_calVar']

# Call the processing function
# (1) add base variables (SiteCode, VisitCode)
base_variables.add_sitecode_column(base_path_maganamed, save_path_baseVar)
base_variables.add_visitcode_column(save_path_baseVar, save_path_baseVar)

# (2) add calculated values
measure_calculation_woCopy.calculate_and_save(save_path_baseVar, save_path_calVar)

# (3) Id


