from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Row

spark = SparkSession\
    .builder\
    .appName('EX5_t2')\
    .master('local')\
    .getOrCreate()

google_reviews_df =spark.read.csv('s3a://data/raw/google_reviews', header=True)

sentiment_arr = [Row(Sentiment='Positive', sentiment_rank=1),
 Row(Sentiment='Neutral', sentiment_rank=0),
 Row(Sentiment='Negative', sentiment_rank=-1)]


sentiments_df = spark.createDataFrame(sentiment_arr)


joined_df = google_reviews_df.join(F.broadcast(sentiments_df), ['Sentiment'])


selected_df = joined_df \
 .select(F.col('App').alias('application_name'),
 F.col('Translated_Review').alias('translated_review') ,
 F.col('sentiment_rank'),
 F.col('Sentiment_Polarity').cast(T.FloatType()).alias('sentiment_polarity') ,
 F.col('Sentiment_Subjectivity').cast(T.FloatType()).alias('sentiment_subjectivity'))


selected_df.show()

selected_df.write.parquet('s3a://data/source/google_reviews', mode='overwrite')
spark.stop()
