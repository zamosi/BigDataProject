from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Row

spark = SparkSession\
    .builder\
    .appName('EX5_t2')\
    .master('local')\
    .getOrCreate()


google_reviews_df = spark.read.parquet('s3a://data/source/google_reviews')

google_reviews_df.show()