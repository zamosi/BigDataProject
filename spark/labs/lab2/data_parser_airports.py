#TASK-1
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T


spark = SparkSession.builder.appName('EX2_airports').master('local').getOrCreate()

airports_raw_df = spark.read.csv('s3a://data/raw/airports/', header=True)

airports_df = airports_raw_df.select(F.col('city'),F.col('state'),F.col('name'),F.col('airport_id').cast("int").alias("airport_id"))


airports_df.write.parquet('s3a://data/source/airports/',mode='overwrite')