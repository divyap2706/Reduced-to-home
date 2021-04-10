from __future__ import print_function

# This is the code filters out the housing data to only include the tristate area
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import format_string
from pyspark.sql.functions import lit
from pyspark.sql.functions import date_format
spark = SparkSession.builder.appName("ZipCodes-Max").getOrCreate()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: ZipCodes <file>", file=sys.stderr)
        exit(-1)
    #"New_Realtor_Listings.csv"
    #"cleaned_NYC_Zips.csv"
    #"cleaned_Metro_Zips.csv"
    nyc = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[1]).rdd.map(lambda x: x[0]).collect()
    metro = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[2]).rdd.map(lambda x: x[0]).collect()
    hotness = spark.read.format('csv').options(header='true', inferschema='true').load(sys.argv[3])
    tristate = nyc + metro

    hotness = hotness.filter(hotness["postal_code"].isin(tristate))
    hotness_nyc = hotness.filter(hotness["postal_code"].isin(nyc)).withColumn('Area', lit('NYC'))
    hotness_metro = hotness.filter(hotness["postal_code"].isin(metro)).withColumn('Area', lit('Metro'))
    hotness = hotness_nyc.union(hotness_metro)
    hotness.coalesce(1).write.option("header", True).csv("NYC_Listings.csv")

    spark.stop()
