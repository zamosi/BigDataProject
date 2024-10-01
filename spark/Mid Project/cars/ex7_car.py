from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T



spark = SparkSession \
 .builder \
 .master("local") \
 .appName('car_ex7') \
 .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
 .getOrCreate()

json_schema = T.StructType([T.StructField('event_id',T.StringType(),True),
                            T.StructField('event_time',T.TimestampType(),True),
                            T.StructField('car_id',T.IntegerType(),True),
                            T.StructField('speed',T.IntegerType(),True),
                            T.StructField('rpm',T.IntegerType(),True),
                            T.StructField('gear',T.IntegerType(),True),
                            T.StructField('driver_id',T.IntegerType(),True),
                            T.StructField('brand_name',T.StringType(),True),
                            T.StructField('model_name',T.StringType(),True),
                            T.StructField('color_name',T.StringType(),True),
                            T.StructField('expected_gear',T.FloatType(),True)])

stream_df = spark \
 .readStream \
 .format('kafka') \
 .option("kafka.bootstrap.servers", "course-kafka:9092") \
 .option("subscribe", "alert-data") \
 .option('startingOffsets', 'earliest') \
 .option("failOnDataLoss", "false") \
 .load() \
 .select(F.col('value').cast(T.StringType()))


parsed_df = stream_df \
 .withColumn('parsed_json', F.from_json(F.col('value'), json_schema)) \
 .select(F.col('parsed_json.*'))

df = parsed_df.agg(
    F.count(F.col('event_id')).alias('num_of_rows'),
    F.sum(F.when(F.col('color_name')==F.lit('Black'),1).otherwise(0)).alias('num_of_black'),
    F.sum(F.when(F.col('color_name')==F.lit('White'),1).otherwise(0)).alias('num_of_white'),
    F.sum(F.when(F.col('color_name')==F.lit('Silver'),1).otherwise(0)).alias('num_of_silver'),
    F.max(F.col('speed')).alias('maximum_speed'),
    F.max(F.col('gear')).alias('maximum_gear'),
    F.max(F.col('rpm')).alias('maximum_rpm')
    )
                   


query = df.writeStream \
    .trigger(processingTime='15 minute') \
    .outputMode("update") \
    .format("console") \
    .start()

query.awaitTermination()