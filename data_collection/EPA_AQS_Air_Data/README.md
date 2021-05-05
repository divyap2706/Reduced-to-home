# EPA_AQS_AIR_Data
This is a simple python script for retrieving data from the EPA AQS Web Service (https://www.epa.gov/aqs).

It utilizes the Daily Data Summary API (https://aqs.epa.gov/aqsweb/documents/data_api.html#daily) to retrieve summary Air Quality data for New York County in the State of New York, for all all parameters defined in the AQI Pollutant Parameter class. (https://aqs.epa.gov/aqsweb/documents/data_api.html#meta)

## Installation
Please note that this requires at least Python 3.6

In the command line, please cd into the directory that this was checked out into.<br/> 
`cd EPA_AQS_Air_data/`

Install a virtual environment.<br/>
`python3 -m virtualenv venv`

Activate the environment.<br/>
`source venv/bin/activate`

Install Required modules.<br/>
`pip install -r requirements.txt`

## Execution
Once all the required modules have been imported, the script can be run with:<br/>
`python -m epa_air_analysis`

## Outputs
Currently the script outputs a single CSV file titled `epa_aqs_air_AQI-POLLUTANTS_2019-2020.csv`. This csv should be uploaded to HDFS for processing by the `epa_air_analysis.py` PySpark job stored in the `src/cleaning_src/Spark_Jobs` folder.
