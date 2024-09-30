from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from time import sleep
from kafka import KafkaProducer
import json

spark = SparkSession \
 .builder \
 .master("local") \
 .appName('car_ex4') \
 .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
 .getOrCreate()

schema_cars = T.StructType([T.StructField('car_id',T.IntegerType(),True),
                            T.StructField('driver_id',T.IntegerType(),True),
                            T.StructField('model_id',T.IntegerType(),True),
                            T.StructField('color_id',T.IntegerType(),True)])

cars = spark.read.schema(schema_cars).parquet('s3a://spark/data/dims/cars',mode='overwrite')


cars_df = cars\
    .withColumn('event_id',F.concat(F.col('car_id').cast('string'),F.unix_timestamp().cast('string')))\
    .withColumn('event_time',F.current_timestamp())\
    .withColumn("speed", (F.floor(F.rand() * (200 - 0 + 0) + 1)).cast("int"))\
    .withColumn("rpm", (F.floor(F.rand() * (8000 - 0 + 0) + 1)).cast("int"))\
    .withColumn("gear", (F.floor(F.rand() * (7 - 1 + 1) + 1)).cast("int"))\
    .select('event_id','event_time','car_id','speed','rpm','gear')



while True:
    
    cars_df = cars\
        .withColumn('event_id',F.concat(F.col('car_id').cast('string'),F.unix_timestamp().cast('string')))\
        .withColumn('event_time',F.current_timestamp())\
        .withColumn("speed", (F.floor(F.rand() * (200 - 0 + 0) + 1)).cast("int"))\
        .withColumn("rpm", (F.floor(F.rand() * (8000 - 0 + 0) + 1)).cast("int"))\
        .withColumn("gear", (F.floor(F.rand() * (7 - 1 + 1) + 1)).cast("int"))\
        .select('event_id','event_time','car_id','speed','rpm','gear')

    # # Show the car DataFrame    
    # event.show(truncate=False)
        
    kafka_writer = cars_df.selectExpr("to_json(struct(*)) AS value")\
        .write\
        .format('kafka')\
        .option("kafka.bootstrap.servers", "course-kafka:9092")\
        .option("topic", "sensors-sample")\
        .option("checkpointLocation", "s3a://spark/checkpoints/final_ex/events")\
        .save()
        
    sleep(1)
              


