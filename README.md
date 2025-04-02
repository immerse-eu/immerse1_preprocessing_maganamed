# immerse1_processing_maganamed

This repository is for processing raw MaganaMed data.
- Adds base variables such as SiteCode and VisitCode
- Adds calculated values for certain MaganaMed forms, based on researcher specifications
- Handles ID processing, including:
  - Merging data from different variants of the same participant
  - Discarding invalid IDs
  - Moving data that was mistakenly saved under the wrong ID to the correct one

Input:
- Unzipped raw MaganaMed data, 
located in the directory specified by the configuration file 
(\data\maganamed\export)
- Reference file (.xlsx) specifying the rules for ID processing, located in the directory specified by the config file (\data\maganamed\)

Output:
Processed MaganaMed data,
saved to the directory specified by the configuration file 
(\data_processed\maganamed)
