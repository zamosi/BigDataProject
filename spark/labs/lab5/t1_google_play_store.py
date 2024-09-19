from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Row

spark = SparkSession\
    .builder\
    .appName('EX5_t1')\
    .master('local')\
    .getOrCreate()

google_apps_raw_df=spark.read.csv('s3a://data/raw/google_apps', header=True)


# google_apps_raw_df.printSchema()

age_limit_arr = [Row(age_limit=18, Content_Rating='Adults only 18+'),
                 Row(age_limit=17, Content_Rating='Mature 17+'),
                 Row(age_limit=12, Content_Rating='Teen'),
                 Row(age_limit=10, Content_Rating='Everyone 10+'),
                 Row(age_limit=0, Content_Rating='Everyone')]
age_limit_df = spark.createDataFrame(age_limit_arr).withColumnRenamed('Content_Rating', 'Content Rating')


df_join_age_limit = google_apps_raw_df.join(F.broadcast(age_limit_df),'Content Rating')
# df_join_age_limit.show()

google_apps_df = df_join_age_limit.select(
    F.col('App').alias('application_name')\
    ,F.col('category').alias('category')\
    ,F.col('Rating').alias('rating')\
    ,F.col('Reviews').cast('float').alias('reviews')\
    ,F.col('Size').alias('size')\
    ,F.regexp_replace(F.col('installs'),'[^0-9]','').cast('double').alias('num_of_installs')\
    ,F.col('Price').cast('double').alias('price')\
    ,F.col('age_limit').cast('double')\
    ,F.col('Genres').alias('genres')\
    ,F.col('Current ver').alias('version')
        )


google_apps_df.write.parquet('s3a://data/source/google_apps/',mode='overwrite')