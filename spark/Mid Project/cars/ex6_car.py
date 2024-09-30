from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T



spark = SparkSession \
 .builder \
 .master("local") \
 .appName('car_ex6') \
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
 .option("subscribe", "samples-enriched") \
 .option('startingOffsets', 'earliest') \
 .option("failOnDataLoss", "false") \
 .load() \
 .select(F.col('value').cast(T.StringType()))


parsed_df = stream_df \
 .withColumn('parsed_json', F.from_json(F.col('value'), json_schema)) \
 .select(F.col('parsed_json.*'))


filter_df = parsed_df.filter((parsed_df.speed>120) &(parsed_df.rpm>6000) &(parsed_df.expected_gear!=parsed_df.gear))
# filter_df = parsed_df.filter((parsed_df.speed>120)&(parsed_df.rpm>6000))

query = filter_df.selectExpr("to_json(struct(*)) AS value")\
 .writeStream \
 .format('kafka') \
 .option("kafka.bootstrap.servers", "course-kafka:9092") \
 .option("topic", "alert-data") \
 .option('startingOffsets', 'earliest')\
 .option('checkpointLocation', 's3a://spark/checkpoints/final_ex/alert-data') \
 .outputMode('append') \
 .start()

query.awaitTermination()

spark.stop()