from __future__ import print_function

# This is the code cleans the zip code data
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
from pyspark.sql.functions import lit
from pyspark.sql.functions import date_format
spark = SparkSession.builder.appName("ZipCodes-Max").getOrCreate()


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: ZipCodes <file>", file=sys.stderr)
        exit(-1)
    #"Zips_In_NYC.csv"
    #"Zips_In_Metro_Area.csv"
    nyc = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1])
    metro = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[2])

    nyc = nyc.select(nyc["ZIP"])
    metro = metro.select(metro["ZIP"])
    nyc = nyc.filter(((nyc["ZIP"] != 11001) & (nyc["ZIP"] != 11040) & (nyc["ZIP"] != 11003)))
    nyc = nyc.select(format_string('%05d', nyc["ZIP"]))
    metro = metro.select(format_string('%05d', metro["ZIP"]))
    nyc.coalesce(1).write.csv("cleaned_NYC_Zips.csv")
    metro.coalesce(1).write.csv("cleaned_Metro_Zips.csv")

    spark.stop()
