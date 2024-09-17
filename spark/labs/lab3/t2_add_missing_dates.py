from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Row
from datetime import date,timedelta

spark = SparkSession\
    .builder\
    .appName('EX3_t2')\
    .master('local')\
    .getOrCreate()

def get_dates_df():
    dummy_df = spark.createDataFrame([Row(dummy='x')])
    in_date_df = dummy_df.select(F.explode(F.sequence(F.lit("2020-01-01").cast(T.DateType()),F.lit("2020-12-31").cast(T.DateType()))).alias('flight_date'))
    return in_date_df

flight_df = spark.read.parquet('s3a://data/stg/flights_matched/')

dates_df = get_dates_df()



dates_full_df = dates_df\
    .withColumn('day_of_week',F.dayofweek(F.col('flight_date')))\
    .withColumn('day_of_month',F.dayofmonth(F.col('flight_date')))

max_date_df = dates_full_df\
    .groupBy(F.col('day_of_week'),F.col('day_of_month'))\
    .agg(F.max(F.col('flight_date'))\
    .alias('flight_date'))

# max_date_df.show(20)

enriched_flights_df = flight_df.join(max_date_df,['day_of_week','day_of_month'])

enriched_flights_df.write.parquet('s3a://data/transformed/flights/',mode='overwrite')

enriched_flights_df.show(20)
spark.stop()
