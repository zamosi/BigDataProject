from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


spark = SparkSession\
    .builder\
    .appName('EX3_t3')\
    .master('local')\
    .getOrCreate()

flights_df = spark.read.parquet('s3a://data/transformed/flights/')
airports_df = spark.read.parquet('s3a://data/source/airports/')

flights_df.printSchema()
airports_df.printSchema()

#Top 10 Airports by Departures
flights_df_agg = flights_df\
    .groupBy(F.col('origin_airport_id'))\
    .agg(F.count(F.lit(1))\
    .alias('number_of_departures'))\
       
flights_df_agg\
    .join(airports_df,flights_df_agg['origin_airport_id']==airports_df['airport_id'],'left')\
    .orderBy('number_of_departures',ascending=False)\
    .select(flights_df_agg['origin_airport_id'],flights_df_agg['number_of_departures'],airports_df['name'].alias('airport_name'))\
    .show(10)


#Top 10 Airports by Arrivals
flights_df_agg = flights_df\
    .groupBy(F.col('dest_airport_id'))\
    .agg(F.count(F.lit(1))\
    .alias('number_of_departures'))\
       
flights_df_agg\
    .join(airports_df,flights_df_agg['dest_airport_id']==airports_df['airport_id'],'left')\
    .orderBy('number_of_departures',ascending=False)\
    .select(flights_df_agg['dest_airport_id'],flights_df_agg['number_of_departures'],airports_df['name'].alias('airport_name'))\
    .show(10)


#Top 10 Flight Routes
flights_df_agg = flights_df\
    .groupBy(F.col('origin_airport_id').alias('source_airport_id'),F.col('dest_airport_id'))\
    .agg(F.count(F.lit(1)).alias('route_count'))

flights_df_agg\
    .join(airports_df.alias('airports_df_src'),flights_df_agg['source_airport_id']==F.col('airports_df_src.airport_id'),'left')\
    .join(airports_df.alias('airports_df_dest'),flights_df_agg['dest_airport_id']==F.col('airports_df_dest.airport_id'),'left')\
    .select(
        flights_df_agg['source_airport_id'],
        flights_df_agg['dest_airport_id'],
        F.col('airports_df_src.name').alias('source_airport_name'),
        F.col('airports_df_dest.name').alias('dest_airport_name'),
        F.concat( F.col('airports_df_dest.name'),F.lit('->'),F.col('airports_df_dest.name')).alias('route_name'),
        flights_df_agg['route_count'])\
    .orderBy('route_count',ascending=False)\
    .show(10)


spark.stop()
