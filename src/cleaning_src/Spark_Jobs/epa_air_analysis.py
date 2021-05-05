import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, year, date_format, col, unix_timestamp


def main():
    """
    Takes data from the 2019-2020 years of EPA AQS data for the AQI Indicator parameters
    and splits them into individual csv files per year and parameter type

    example command to call this script from HDFS:
    spark-submit final_project/src/epa_air_analysis.py /user/ab7289/final_project/epa_aqs_air_AQI-POLLUTANTS_2019-2020.csv
    """
    if len(sys.argv) != 2:
        print("Usage: epa_air_analysis.py <file", sys.stderr)
        exit(-1)

    spark = SparkSession.builder.appName("EPA AIR Analysis").getOrCreate()

    air_df = (
        spark.read.format("csv").options(inferSchema="true", header="true")
        # .load("/user/ab7289/final_project/epa_aqs_air_AQI-POLLUTANTS_2019-2020.csv")
        .load(sys.argv[1])
    )

    generate_param_sheets(air_df)
    gen_pm25_for_real_estate(air_df)
    spark.stop()


def generate_param_sheets(air_df):
    """
    Handles splitting up the EPA Air data spreadsheet into individual spreadsheets
    per parameter and per year. Additionally compiles all the parameters' data
    after transformation into a single sheet for easy visualization
    """
    # split out the 2019 data
    air_2019_df = air_df.filter(year("date_local") == lit(2019))
    # split out the 2020 data
    air_2020_df = air_df.filter(year("date_local") == lit(2020))

    outputs_2019 = {
        "pm25a_2019_avg_df": "parameter like 'Acceptable PM2.5%' AND year(date_local) = 2019 AND sample_duration like '24-HR%'",
        "ozone_2019_avg_df": "parameter like 'Ozone' AND year(date_local) = 2019 AND sample_duration like '%8-HR%'",
        "pm25_local_2019_avg_df": "parameter like 'PM2.5 - Local%' AND year(date_local) = 2019 AND sample_duration like '24 HOUR'",
        "pm10_2019_avg_df": "parameter like 'PM10%' AND year(date_local) = 2019 AND sample_duration like '24 HOUR'"
        # "all_parm_2019_df": "year(date_local) = 2019 AND aqi IS NOT NULL",
    }
    outputs_2020 = {
        "pm25a_2020_avg_df": "parameter like 'Acceptable PM2.5%' AND year(date_local) = 2020 AND sample_duration like '24-HR%'",
        "ozone_2020_avg_df": "parameter like 'Ozone' AND year(date_local) = 2020 AND sample_duration like '%8-HR%'",
        "pm25_local_2020_avg_df": "parameter like 'PM2.5 - Local%' AND year(date_local) = 2020 AND sample_duration like '24 HOUR'",
        "co_2020_avg_df": "parameter like 'Carbon monoxide' AND sample_duration like '8-HR%'",
        "pm10_2020_avg_df": "parameter like 'PM10%' AND year(date_local) = 2020 AND sample_duration like '24 HOUR'"
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


def gen_pm25_for_real_estate(air_df):
    """
    creates a spreadsheet of the PM2.5 parameter data so that
    the 2019 data can be easily visualized against the 2020 data
    """
    pm25_local_df = air_df.filter(
        "parameter like 'PM2.5 - Local%' AND sample_duration like '24 HOUR'"
    ).select("parameter", "date_local", "aqi")
    pm25_acc_df = air_df.filter(
        "parameter like 'Acceptable PM2.5%' AND sample_duration like '24-HR%'"
    ).select("parameter", "date_local", "aqi")

    local_param_name = pm25_local_df.select("parameter").first()["parameter"]
    acc_param_name = pm25_acc_df.select("parameter").first()["parameter"]

    pm25_agg = agg_pm25(pm25_local_df, local_param_name).union(
        agg_pm25(pm25_acc_df, acc_param_name)
    )

    to_csv(pm25_agg, "pm25_aggregated")


def agg_pm25(filtered_frame, param_name):
    """
    aggregates the PM2.5 parameters over 2019 and 2020 so that they can be
    compared against each other
    """
    agg_frame = (
        filtered_frame.withColumn(
            "yearmonth", date_format(col("date_local"), "yyyy-MM-01")
        )
        .groupBy("yearmonth")
        .agg({"aqi": "avg"})
        .withColumnRenamed("avg(aqi)", "aqi")
        .withColumn("parameter", lit(param_name))
        .withColumn("year", date_format(col("yearmonth"), "yyyy"))
        .withColumn("month", date_format(col("yearmonth"), "M"))
    )
    return agg_frame


def aggregate(df, filter_query, output_name):
    """
    creates a new column called 'month', aggregates the aqi and arithmetic_mean
    by month, then outputs to csv

    returns the new aggregated dataframe
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
        filtered_df.withColumn(
            "date_month", date_format(col("date_local"), "yyyy-MM-01")
        )
        .groupBy("date_month")
        .agg({"arithmetic_mean": "avg", "aqi": "avg"})
        .withColumnRenamed("avg(aqi)", "aqi")
        .withColumnRenamed("avg(arithmetic_mean)", "arithmetic_mean")
        .withColumn("units_of_measure", lit(units))
        .withColumn("parameter", lit(param))
        .withColumn("year", date_format(col("date_month"), "yyyy"))
        .withColumn("month", date_format(col("date_month"), "M"))
    )
    to_csv(agg_df, output_name)
    return agg_df


def to_csv(df, output_name):
    """
    Takes the supplied dataframe and output filename and creates
    a csv file in the final_rpoject/output_air/ directory
    """
    df.coalesce(1).write.option("header", True).csv(
        "epa_air/{}.csv".format(output_name)
    )


if __name__ == "__main__":
    main()
