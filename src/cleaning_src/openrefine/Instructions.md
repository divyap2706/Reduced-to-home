# OpenRefine DataCleaning Instructions

## Monthly Energy consumption data

Few steps to clean Energy consumption data using Openrefine
The energy consumption dataset has 3 sheets for 2019, 2020 and 2021 consumption.
2019 has been cleaned seperately and 2020-2021 have been cleaned together for comparative analysis of the consumption pre and post covid.

### Execution

1) Upload the Energy consumption monthly 2019,2020 and 2021 dataset in OpenRefine
2) Create a new project
3) Select only 2019 sheet from the dataset.
4) Click on Undo/Redo tab and then on the apply tab.
5) Copy paste the json commands from the 2019_energy consumption.json file and click on perform operations.
6) Now select the 2020 and 2021 sheet from the dataset.
7) Go through the same steps and copy paste the json commands from 2020-2021 energy consumption.json file and click on perform operations.

### Outputs
The cleaned data sets has been uploaded in the Cleaned_Data folder as 2019.csv and 2020_2021_energy consumption.csv
