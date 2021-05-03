# Requirements
## Spark job files
Priot to submitting the sparkk job, make sure that the machine or cluster you are using has the libraries which are needed by running:
```
pip install -r requirements.txt
```

# Submit the Spark Job
On the cluster that is running spark, navigate to a folder that contains the spark job file and the dataset required for that file. Then for example, run:
```
spark-submit Tristate_Realtor_Hotness.py cleaned_NYC_Zips.csv cleaned_Metro_Zips.csv New_Realtor_Hotness.csv
```

Where Tristate_Realtor_Hotness.py is the job we are running, and cleaned_NYC_Zips.csv, cleaned_Metro_Zips.csv, and New_Realtor_Hotness.csv are the input datasets. 
Some of the jobs require more than one dataset as input, and the order that these datasets are passed as arguments matters. Please see the code for each job to check how the arguments should be passed in. 
