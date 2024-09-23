import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from kafka import KafkaProducer

spark = SparkSession.builder.master("local").appName('EX5_t3').getOrCreate()


google_reviews_df = spark.read.parquet('s3a://data/source/google_reviews')

data = google_reviews_df.toJSON()

producer = KafkaProducer(bootstrap_servers='course-kafka:9092', value_serializer=lambda v: v.encode('utf-8'))

i = 0
for json_data in data.collect():
    i = i + 1
    producer.send(topic='gps-user-review-source', value=json_data)
    if i == 50:
        producer.flush()
        time.sleep(5)
        i = 0

producer.close()
spark.stop()