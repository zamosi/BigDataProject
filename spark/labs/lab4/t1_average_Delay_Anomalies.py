from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

spark = SparkSession\
    .builder\
    .appName('ex4_t1_anomalies_detection')\
    .master('local')\
    .config("spark.driver.memory", "4g")\
    .getOrCreate()

flights_df = spark.read.parquet('s3a://data/transformed/flights/')

all_history_window = Window.partitionBy("Carrier").orderBy("flight_date").rowsBetween(Window.unboundedPreceding, Window.currentRow)

flights_df\
    .withColumn('avg_till_now',F.avg(F.col('arr_delay')).over(all_history_window))\
    .withColumn('avg_diff_percent',(F.abs(F.col('arr_delay'))-F.abs(F.col('avg_till_now')))/F.abs(F.col('avg_till_now')))\
    .filter(F.col('avg_diff_percent')>3)\
    .show()
    