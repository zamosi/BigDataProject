from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T

spark = SparkSession \
 .builder \
 .master("local") \
 .appName('car_ex1') \
 .getOrCreate()

schema = T.StructType([
 T.StructField('model_id', T.IntegerType()),
 T.StructField('car_brand', T.StringType()),
 T.StructField('car_model', T.StringType())
 ])

data = [(1,'Mazda','3'),
        (2,'Mazda','6'),
        (3,'Toyota','Corolla'),
        (4,'Hyundai','i20'),
        (5,'Kia','Sportage'),
        (6,'Kia','Rio'),
        (7,'Kia','Picanto')]

df = spark.createDataFrame(data,schema = schema)

# df.show()

df.write.parquet('s3a://spark/data/dims/car_models',mode='overwrite')


spark.stop()