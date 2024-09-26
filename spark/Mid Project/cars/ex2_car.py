from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = SparkSession \
 .builder \
 .master("local") \
 .appName('car_ex2') \
 .getOrCreate()

schema = T.StructType([
 T.StructField('color_id  ', T.IntegerType()),
 T.StructField('color_name', T.StringType())
 ])

data = [(1,'Black'),
        (2,'Red'),
        (3,'Gray'),
        (4,'White'),
        (5,'Green'),
        (6,'Blue'),
        (7,'Pink')]

df = spark.createDataFrame(data,schema = schema)

# df.show()

df.write.parquet('s3a://spark/data/dims/car_colors',mode='overwrite')

spark.stop()