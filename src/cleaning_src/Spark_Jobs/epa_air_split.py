import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, year, date_format, col


def main():
    """
    Takes data from the 2019-2020 years of EPA AQS data for the AQI Indicator parameters
    and splits them into individual csv files per year and parameter type

    example command to call this script from HDFS:
    spark-submit final_project/src/epa_air_split.py /user/ab7289/final_project/epa_aqs_air_AQI-POLLUTANTS_2019-2020.csv
    """
    if len(sys.argv) != 2:
        print("Usage: epa_air_split.py <file", sys.stderr)
        exit(-1)

    spark = SparkSession.builder.appName("EPA AIR Splitter").getOrCreate()

    air_df = (
        spark.read.format("csv").options(inferSchema="true", header="true")
        # .load("/user/ab7289/final_project/epa_aqs_air_AQI-POLLUTANTS_2019-2020.csv")
        .load(sys.argv[1])
    )

    # split out the 2019 data
    air_2019_df = air_df.filter(year("date_local") == lit(2019))
    # split out the 2020 data
    air_2020_df = air_df.filter(year("date_local") == lit(2020))

    outputs_2019 = {
        "pm25a_2019_avg_df": "parameter like 'Acceptable PM2.5%' AND year(date_local) = 2019 AND sample_duration like '24-HR%'",
        "ozone_2019_avg_df": "parameter like 'Ozone' AND year(date_local) = 2019 AND sample_duration like '%8-HR%'",
        "pm25_local_2019_avg_df": "parameter like 'PM2.5 - Local%' AND year(date_local) = 2019 AND sample_duration like '24 HOUR'",
        # "all_parm_2019_df": "year(date_local) = 2019 AND aqi IS NOT NULL",
    }
    outputs_2020 = {
        "pm25a_2020_avg_df": "parameter like 'Acceptable PM2.5%' AND year(date_local) = 2020 AND sample_duration like '24-HR%'",
        "ozone_2020_avg_df": "parameter like 'Ozone' AND year(date_local) = 2020 AND sample_duration like '%8-HR%'",
        "pm25_local_2020_avg_df": "parameter like 'PM2.5 - Local%' AND year(date_local) = 2020 AND sample_duration like '24 HOUR'",
        "co_2020_avg_df": "parameter like 'Carbon monoxide' AND sample_duration like '8-HR%'",
        # "all_parm_2020_df": "year(date_local) = 2020 and aqi IS NOT NULL",
    }

    frames_2019 = []
    frames_2020 = []
    for k, v in outputs_2019.items():
        frames_2019.append(aggregate(air_2019_df, v, k))

    for k, v in outputs_2020.items():
        frames_2020.append(aggregate(air_2020_df, v, k))

    # union all the filtered datasets so we can view all the parameters
    # in one graph
    all_parms_2019_df = frames_2019[0]
    for i in range(1, len(frames_2019)):
        all_parms_2019_df = all_parms_2019_df.union(frames_2019[i])

    all_parms_2020_df = frames_2020[0]
    for i in range(1, len(frames_2020)):
        all_parms_2020_df = all_parms_2020_df.union(frames_2020[i])

    to_csv(all_parms_2019_df, "all_parms_2019_df")
    to_csv(all_parms_2020_df, "all_parms_2020_df")

    spark.stop()


def aggregate(df, filter_query, output_name):
    """
    creates a new column called 'month', aggregates the aqi and arithmetic_mean
    by month, then outputs to csv
    """
    # filter the whole spreadsheet down to just the parameter, year, and columns
    # that we're interested in
    filtered_df = df.filter(filter_query).select(
        "parameter", "date_local", "units_of_measure", "arithmetic_mean", "aqi"
    )
    # pull out the unit of measure and parameter name so we can reinsert it
    # for graphing all the parameters after aggregation
    units = filtered_df.select("units_of_measure").first()["units_of_measure"]
    param = filtered_df.select("parameter").first()["parameter"]
    # group the data by month, and take the average of the aqi and arithmetic_mean
    agg_df = (
        filtered_df.withColumn("month", date_format(col("date_local"), "M/yyyy"))
        .groupBy("month")
        .agg({"arithmetic_mean": "avg", "aqi": "avg"})
        .withColumnRenamed("avg(aqi)", "aqi")
        .withColumnRenamed("avg(arithmetic_mean)", "arithmetic_mean")
        .withColumn("units_of_measure", lit(units))
        .withColumn("parameter", lit(param))
    )
    to_csv(agg_df, output_name)
    return agg_df


def to_csv(df, output_name):
    """
    Takes the supplied dataframe and output filename and creates
    a csv file in the final_rpoject/output_air/ directory
    """
    df.coalesce(1).write.option("header", True).csv(
        "final_project/output_air/{}.csv".format(output_name)
    )


if __name__ == "__main__":
    main()
