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
        "all_parm_2019_df": "year(date_local) = 2019 AND aqi IS NOT NULL",
    }
    outputs_2020 = {
        "pm25a_2020_avg_df": "parameter like 'Acceptable PM2.5%' AND year(date_local) = 2020 AND sample_duration like '24-HR%'",
        "ozone_2020_avg_df": "parameter like 'Ozone' AND year(date_local) = 2020 AND sample_duration like '%8-HR%'",
        "pm25_local_2020_avg_df": "parameter like 'PM2.5 - Local%' AND year(date_local) = 2020 AND sample_duration like '24 HOUR'",
        "co_2020_avg_df": "parameter like 'Carbon monoxide' AND sample_duration like '8-HR%'",
        "all_parm_2020_df": "year(date_local) = 2020 and aqi IS NOT NULL",
    }

    for k, v in outputs_2019.items():
        aggregate(air_2019_df, v, k)

    for k, v in outputs_2020.items():
        aggregate(air_2020_df, v, k)

    # get PM2.5 values for 2019
    # pm25a_2019_avg_df = air_2019_df.filter(
    #     "parameter like 'Acceptable PM2.5%' AND year(date_local) = 2019 AND sample_duration like '24-HR%'"
    # ).select("parameter", "date_local", "units_of_measure", "arithmetic_mean", "aqi")
    # get PM2.5 values for 2020
    # pm25a_2020_avg_df = air_2020_df.filter(
    #     "parameter like 'Acceptable PM2.5%' AND year(date_local) = 2020 AND sample_duration like '24-HR%'"
    # ).select("parameter", "date_local", "units_of_measure", "arithmetic_mean", "aqi")

    # get ozone values for 2019
    # ozone_2019_avg_df = air_2019_df.filter(
    #     "parameter like 'Ozone' AND year(date_local) = 2019 AND sample_duration like '%8-HR%'"
    # ).select("parameter", "date_local", "units_of_measure", "arithmetic_mean", "aqi")
    # get ozone values for 2020
    # ozone_2020_avg_df = air_2020_df.filter(
    #     "parameter like 'Ozone' AND year(date_local) = 2020 AND sample_duration like '%8-HR%'"
    # ).select("parameter", "date_local", "units_of_measure", "arithmetic_mean", "aqi")

    # get Local PM2.5 values for 2019
    # pm25_local_2019_avg_df = air_2019_df.filter(
    #     "parameter like 'PM2.5 - Local%' AND year(date_local) = 2019 AND sample_duration like '24 HOUR'"
    # ).select("parameter", "date_local", "units_of_measure", "arithmetic_mean", "aqi")
    # get Local PM2.5 values for 2020
    # pm25_local_2020_avg_df = air_2020_df.filter(
    #     "parameter like 'PM2.5 - Local%' AND year(date_local) = 2020 AND sample_duration like '24 HOUR'"
    # ).select("parameter", "date_local", "units_of_measure", "arithmetic_mean", "aqi")

    # get Carbon Monoxide values for 2020, since there is no 2019 data
    # co_2020_avg_df = air_2020_df.filter(
    #     "parameter like 'Carbon monoxide' AND sample_duration like '8-HR%'"
    # ).select("parameter", "date_local", "units_of_measure", "arithmetic_mean", "aqi")

    # get data for all parameters for 2019 and 2020
    # all_parm_2019_df = air_2019_df.filter(
    #     "year(date_local) = 2019 AND aqi IS NOT NULL"
    # ).select("parameter", "date_local", "units_of_measure", "arithmetic_mean", "aqi")

    # all_parm_2020_df = air_2020_df.filter(
    #     "year(date_local) = 2020 and aqi IS NOT NULL"
    # ).select("parameter", "date_local", "units_of_measure", "arithmetic_mean", "aqi")

    # write output so we can graph it

    spark.stop()


def aggregate(df, filter_query, output_name):
    """
    creates a new column called 'month', aggregates the aqi and arithmetic_mean
    by month, then outputs to csv
    """
    filtered_df = (
        df.filter(filter_query)
        .select("parameter", "date_local", "units_of_measure", "arithmetic_mean", "aqi")
        .withColumn("month", date_format(col("date_local"), "M/yyyy"))
        .groupBy("month")
        .agg({"arithmetic_mean": "avg", "aqi": "avg"})
        .withColumn(
            "units_of_measure",
            lit(df.select("units_of_measure").first()["units_of_measure"]),
        )
        .withColumn("parameter", lit(df.select("parameter").first()["parameter"]))
    )
    to_csv(filtered_df, output_name)


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
