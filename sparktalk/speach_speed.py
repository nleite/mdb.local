#! /usr/bin/env python
from __future__ import print_function
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
from pyspark import sql
from collections import namedtuple


if __name__ == "__main__":

    print("HELLO")
    # create a spark context
    sc = SparkContext(appName="NetworkWordCount")

    # create spark streaming context
    ssc = StreamingContext(sc, 1)

    # create spark sql context
    sqlContext = sql.SQLContext(sc)

    # open a socket stream to read incoming text
    socket_stream = ssc.socketTextStream("127.0.0.1", 3999)

    # create reading window for every 30 seconds
    lines = socket_stream.window( 30 )

    # set namedtuple for found words and their count
    fields = ("word", "count" )
    WordCount = namedtuple( 'WordCount', fields )

    # collect all per 30 second rdd

    rdds = (lines.flatMap( lambda text: text.split( " " ) )
      .map( lambda word: ( word.lower(), 1 ) )
      .reduceByKey( lambda a, b: a + b )
      .map( lambda rec: WordCount(rec[0], rec[1]) )
      #.filter(lambda rec: len(rec) > 3 )
      )


    # for each defined RDD store the results in MongoDB
    rdds.foreachRDD(
        lambda rdd: rdd.toDF()
            .write.format("com.mongodb.spark.sql")
            .mode("overwrite")
            .save()
    )


    ssc.start()
    ssc.awaitTerminationOrTimeout(10)
    ssc.stop()
