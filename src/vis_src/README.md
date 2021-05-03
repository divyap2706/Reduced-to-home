# Requirements

## Notebook files
Prior to running the notebooks, be sure to run: 
```
pip install -r requirements.txt
```
as described in the README for the entire project. 

## Tableau Workbook
Make sure to install the latest version of Tableau Public from 
https://public.tableau.com/en-us/s/

# Run the Notebook Files
Open a terminal at a directory folder that contains the notebook file and the dataset required for that file. 
For example, the Bridges_analysis.ipynb notebook uses the New_Bridges_Tunnels.csv file which is produced by the BigData_BridgesTunnels_DataCleaning.ipynb notebook.
Then run the following command from the terminal:
```
jupyter notebook
```
From there, the notebook file can be run in the browser using the Jupyter Notebook page that opens.

# Run the Tableau Workbook
To run the published workbook, visit https://public.tableau.com/profile/max.christ4104#!/vizhome/shared/FTWSHZR2P.

To run the notebook locally, open Tableau Public Desktop on your local machine. Then open the 'NYC Metro Area Housing Data.twbx' file in Tableau Public Desktop.

Note: In order to examine the Data Source tab in Tableau Public Desktop on your local machine, you will need to inform the program where the folowing files are located locally:
1. The contents of Big-Data-Project-X/data/Spatial_Data_For_Tableau_Viz/
2. avg_hotness_change.csv

avg_hotness_change.csv is generated from Big-Data-Project-X/src/analysis_src/Hotness_percent_change.ipynb.
