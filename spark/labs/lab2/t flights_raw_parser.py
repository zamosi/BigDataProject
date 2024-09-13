#TASK-3
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T



spark = SparkSession.builder.appName('EX2_flights_parser').master('local').getOrCreate()

flights_parser_raw_df = spark.read.csv('s3a://data/raw/flights_raw/', header=True)

flights_parser_df= flights_parser_raw_df.select(
    F.col('DayofMonth').cast('int').alias('day_of_month')\
    ,F.col('DayOfWeek').cast('int').alias('day_of_week')\
    ,F.col('Carrier')\
    ,F.col('OriginAirportID').cast('int').alias('origin_airport_id')\
    ,F.col('DestAirportID').cast('int').alias('dest_airport_id')\
    ,F.col('DepDelay').cast('int').alias('dep_delay')\
    ,F.col('ArrDelay').cast('int').alias('arr_delay')
        )

flights_parser_df.write.parquet('s3a://data/source/flights_raw/',mode='overwrite')
