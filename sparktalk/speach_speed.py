#! /usr/bin/env python
from __future__ import print_function
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
from pyspark import sql
from collections import namedtuple


def store_data(rdd):
    if len(rdd.collect())>0:
        rdd.toDF().write.format("com.mongodb.spark.sql").mode("overwrite").save()

def store_bucket_data(time, rdd):
    if len(rdd.collect())>0:
        Timeline = namedtuple("Timeline", ("time", "word", "count"))
        newrdd = rdd.map(lambda rec: Timeline(time, rec[0], rec[1]) )
        newrdd.toDF().write.format("com.mongodb.spark.sql").mode("overwrite").save()

if __name__ == "__main__":

    # create a spark context
    sc = SparkContext(appName="NetworkWordCount")

    # create spark streaming context
    ssc = StreamingContext(sc, 1)

    # create spark sql context
    sqlContext = sql.SQLContext(sc)

    # open a socket stream to read incoming text
    socket_stream = ssc.socketTextStream("127.0.0.1", 3999)

    # create reading interval window
    lines = socket_stream.window( 10 )

    # set namedtuple for found words and their count
    fields = ("word", "count" )
    WordCount = namedtuple( 'WordCount', fields )

    #
    words = lines.flatMap(lambda line: line.split(" "))
    wordCounts = words.map(lambda word: (word.lower().strip(), 1)).reduceByKey(lambda x, y: x + y)
    wordCountsFiltered = wordCounts.filter(lambda rec: len(rec[0]) > 3)
    wordCountsTuppled = wordCountsFiltered.map(lambda rec: WordCount(rec[0], rec[1]) )

    # store data into MongoDB
    wordCountsTuppled.foreachRDD(store_bucket_data)

    # start context and set timeout value
    ssc.start()
    ssc.awaitTerminationOrTimeout(10)
    ssc.stop()
