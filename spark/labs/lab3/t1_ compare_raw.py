from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = SparkSession\
    .builder\
    .appName('EX3')\
    .master('local')\
    .config("spark.driver.memory", "4g") \
    .getOrCreate()

flights_df = spark.read.parquet('s3a://data/source/flights/')
flights_row_df = spark.read.parquet('s3a://data/source/flights_raw/')

flights_distinct_df = flights_df.dropDuplicates()
flights_row_distinct_df = flights_row_df.dropDuplicates()

match_flights = flights_distinct_df.intersect(flights_row_distinct_df)

unmatch_flights = flights_distinct_df.subtract(flights_row_distinct_df).withColumn('source_of_data',F.lit('flights'))
unmatch_flights_row = flights_row_distinct_df.subtract(flights_distinct_df).withColumn('source_of_data',F.lit('flights_row'))

unmatch_flights = unmatch_flights.union(unmatch_flights_row)

match_flights.write.parquet('s3a://data/stg/flights_matched/',mode='overwrite')

unmatch_flights.write.parquet('s3a://data/stg/flights_unmatched/',mode='overwrite')

spark.stop()