# This file will aggregate bridge and tunnel data by week

from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import weekofyear
from pyspark.sql import functions as f
from pyspark.sql.functions import concat_ws

spark = SparkSession.builder.appName("Agg_Bridges-Max").getOrCreate()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: Agg_Bridges <file>", file=sys.stderr)
        exit(-1)
    #"New_Bridges_Tunnels.csv"
    bridges = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
    bridges = bridges.withColumn("month", f.substring("Date", 0, 2)).withColumn("day", f.substring("Date", 4, 2)).withColumn("Year", f.substring("Date", 7, 4))
    bridges = bridges.select(concat_ws('-', bridges.Year, bridges.month, bridges.day).alias("NewDate"), "# Vehicles - E-ZPass", "# Vehicles - VToll", "Direction", "Year")
    bridges = bridges.withColumn('Week_of_year', weekofyear(bridges["NewDate"]))
    bridges = bridges.select((bridges["# Vehicles - E-ZPass"] + bridges["# Vehicles - VToll"]).alias('Total_Vehicles'), "Direction", "Year", "Week_of_year")
    bridges = bridges.groupBy('Week_of_year', 'Year', 'Direction').sum('Total_Vehicles').alias('Sum_Total_Vehicles').orderBy('Year', 'Week_of_year')

    bridges.coalesce(1).write.option("header", True).csv("Weekly_Volume_Bridges_Tunnels.csv")

    spark.stop()
