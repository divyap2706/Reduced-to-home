# Requirements

## Notebook files
Prior to running the notebooks, be sure to run: 
```
pip install -r requirements.txt
```
as described in the README for the entire project. 

## Spark job files
Priot to submitting the sparkk job, make sure that the machine or cluster you are using has the libraries which are needed by running:
```
pip install -r requirements.txt
```

# Run the Notebook Files
Open a terminal at a directory folder that contains the notebook file and the dataset required for that file. 
For example, the Hotness_percent_change.ipynb notebook uses the NYC_Hotness.csv file which is produced by the Big-Data-Project-X/src/cleaning_src/Spark_Jobs/Tristate_Realtor_Hotness.py spark job.
Then run the following command from the terminal:
```
jupyter notebook
```
From there, the notebook file can be run in the browser using the Jupyter Notebook page that opens.

# Submit the Spark Job
On the cluster that is running spark, navigate to a folder that contains the spark job file and the dataset required for that file. Then for example, run:
```
spark-submit Bridges_analysis.py New_Bridges_Tunnels.csv
```
Where Bridges_analysis.py is the job we are running, and New_Bridges_Tunnels.csv is the input data. 
