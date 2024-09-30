from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from time import sleep


spark = SparkSession \
 .builder \
 .master("local") \
 .appName('car_ex5') \
 .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2') \
 .getOrCreate()


schema_car_models = T.StructType([T.StructField('model_id',T.IntegerType(),True),
                                  T.StructField('car_brand',T.StringType(),True),
                                  T.StructField('car_model',T.StringType(),True)])


schema_car_colors = T.StructType([T.StructField('color_id', T.IntegerType(),True),
                                T.StructField('color_name', T.StringType(),True)])

schema_cars=  T.StructType([T.StructField('car_id',T.IntegerType(),True),
                            T.StructField('driver_id',T.IntegerType(),True),
                            T.StructField('model_id',T.IntegerType(),True),
                            T.StructField('color_id',T.IntegerType(),True)])

car_models_df = spark.read.schema(schema_car_models).parquet('s3a://spark/data/dims/car_models',mode='overwrite')
car_colors_df = spark.read.schema(schema_car_colors).parquet('s3a://spark/data/dims/car_colors',mode='overwrite')
cars_df = spark.read.schema(schema_cars).parquet('s3a://spark/data/dims/cars',mode='overwrite')


json_schema = T.StructType([T.StructField('event_id',T.StringType(),True),
                            T.StructField('event_time',T.TimestampType(),True),
                            T.StructField('car_id',T.IntegerType(),True),
                            T.StructField('speed',T.IntegerType(),True),
                            T.StructField('rpm',T.IntegerType(),True),
                            T.StructField('rpgearm',T.IntegerType(),True)])

stream_df = spark \
 .readStream \
 .format('kafka') \
 .option("kafka.bootstrap.servers", "course-kafka:9092") \
 .option("subscribe", "sensors-sample") \
 .option('startingOffsets', 'earliest') \
 .load() \
 .select(F.col('value').cast(T.StringType()))


parsed_df = stream_df \
 .withColumn('parsed_json', F.from_json(F.col('value'), json_schema)) \
 .select(F.col('parsed_json.*'))


join_df = parsed_df.join(F.broadcast(cars_df),'car_id')\
                    .join(F.broadcast(car_models_df),'model_id')\
                    .join(F.broadcast(car_colors_df),'color_id')\
                    .withColumn('brand_name',F.col('car_brand'))\
                    .withColumn('model_name',F.col('car_model'))\
                    .withColumn('expected_gear',F.col('speed')/F.lit(30).cast('int'))\
                    .select('driver_id','brand_name','model_name','color_name','expected_gear')
 



query = join_df.selectExpr("to_json(struct(*)) AS value")\
 .writeStream \
 .format('kafka') \
 .option("kafka.bootstrap.servers", "course-kafka:9092") \
 .option("topic", "samples-enriched") \
 .option('checkpointLocation', 's3a://spark/checkpoints/final_ex/samples-enriched2') \
 .outputMode('append') \
 .start()

query.awaitTermination()

spark.stop()