import findspark
import os

findspark.init('/home/ubuntu/spark')

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession.builder \
    .appName("StructuredSocketWordCount") \
    .master('spark://master:7077') \
    .getOrCreate()
    
lines = spark \
    .readStream \
    .format("socket") \
    .option("host", "master") \
    .option("port", 20332) \
    .load()
    
words = lines.select(
    explode(
        split(lines.value, " ")
    ).alias("word")
)

wordCounts = words.groupBy("word").count()

query = wordCounts \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()

query.awaitTermination()
