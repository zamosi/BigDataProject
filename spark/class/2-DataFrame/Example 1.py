'''
Each record in the "dessert" dataset describes a group visit at a restaurant.
Read the data and answer the questions below.
drop the id
change columns:
'day.of.week' -> 'weekday'
'num.of.guest's -> 'num_of_guests'
'dessert' -> 'purchase'
'hour' ->  'shift'
'''
from pyspark.sql import SparkSession
import os
os.chdir(r'/home/developer/projects/spark-course-python/spark_course_python/projects/spark/class/2-DataFrame')
spark = SparkSession.builder\
  .getOrCreate()
# Read a csv file

dessert = spark.read.csv(r"dessert.csv",header=True, inferSchema=True)\
  .drop('id')\
  .withColumnRenamed('day.of.week', 'weekday') \
  .withColumnRenamed('num.of.guests', 'num_of_guests')\
  .withColumnRenamed('dessert', 'purchase')\
  .withColumnRenamed('hour', 'shift')\


dessert.show(5)
dessert.printSchema()

