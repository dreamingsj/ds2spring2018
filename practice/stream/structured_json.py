import os
import findspark

findspark.init('/home/ubuntu/spark')

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
from pyspark.sql.functions import col, from_json, get_json_object
from pyspark.sql.types import *

# Make a spark sql session
spark = SparkSession.builder \
    .appName("StructuredSocketWordCount") \
    .master('spark://master:7077') \
    .getOrCreate()
    
# Get the json-formatted data from Kafka stream
kafka_movies = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "147.46.216.122:9092") \
    .option("subscribe", "movie") \
    .load()
    
# Define the relational schema
schema = StructType().add("title", StringType()).add("genre", StringType()).add("year", LongType())

# Change the JSON events into relational tuples
relational_movies = kafka_movies.select([get_json_object(col("value").cast("string"), "$.{}".format(c)).alias(c)
    for c in ["title", "genre", "year"]])

# Change the type of year from string to integer
relational_movies = relational_movies.select(col("title"), col("genre"), relational_movies.year.cast('integer').alias('year'))
# Select the movie titles with year < 2000
results = relational_movies.select("title").where("year < 2000")

query = results \
    .writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination(60)
