from __future__ import print_function

# This is the code for looking for Zip Codes that appear in both lists.
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
    nyc = nyc.intersect(metro)
    nyc = nyc.select(format_string('%d', nyc["ZIP"]))
    nyc.write.save('zipCodeIntersect.out', format='text')

    spark.stop()

# It ultimately finds 3 zipcodes, and all three should only be part of the
#   metro data, beccause they are part of Nassau County.
# Those zipcodes are: 11001
#                     11040
#                     11003
