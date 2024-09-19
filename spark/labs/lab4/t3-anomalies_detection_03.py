from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

spark = SparkSession\
    .builder\
    .appName('ex4_t3_anomalies_detection')\
    .master('local')\
    .config("spark.driver.memory", "4g")\
    .getOrCreate()

flights_df = spark.read.parquet('s3a://data/transformed/flights/')

grouped_df = flights_df \
 .groupBy(F.col('Carrier'), F.window(F.col('flight_date'), '10 days', '1 day').alias('date_window')) \
 .agg(F.sum(F.col('dep_delay') + F.col('arr_delay')).alias('total_delay'))


structured_df = grouped_df \
 .select(F.col('Carrier'),
 F.col('date_window.start').alias('start_range'),
 F.col('date_window.end').alias('end_range'),
 F.col('total_delay'))

sliding_range_window = Window.partitionBy(F.col('Carrier')).orderBy(F.col('start_range'))

change_df = structured_df \
 .withColumn('last_window_delay', F.lag(F.col('total_delay')).over(sliding_range_window)) \
 .withColumn('change_percent', F.abs(F.lit(1.0) - (F.col('total_delay') / 
F.col('last_window_delay'))))


significant_changes_df = change_df.where(F.col('change_percent') > F.lit(0.3))
significant_changes_df.show(100)
