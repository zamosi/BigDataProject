from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = SparkSession \
 .builder \
 .master("local") \
 .appName('car_ex3') \
 .getOrCreate()


df_car = spark.range(20)\
    .withColumn("car_id", (F.floor(F.rand() * (9999999 - 1000000 + 1) + 1000000)).cast("int"))\
    .withColumn("driver_id", (F.floor(F.rand() * (999999999 - 100000000 + 1) + 100000000)).cast("int"))\
    .withColumn("model_id", (F.floor(F.rand() * (7 - 1 + 1) + 1)).cast("int"))\
    .withColumn("color_id", (F.floor(F.rand() * (7 - 1 + 1) + 1)).cast("int"))\
    .select('car_id','driver_id','model_id','color_id')

# df_car.show()

df_car.write.parquet('s3a://spark/data/dims/cars',mode='overwrite')

spark.stop()