"""
To run this script use the following connectors and 
spark-submit --conf "spark.mongodb.input.uri=mongodb://ec2-54-184-196-78.us-west-2.compute.amazonaws.com/pymongo_test.myCollection"               --conf "spark.mongodb.output.uri=mongodb://ec2-54-184-196-78.us-west-2.compute.amazonaws.com/pymongo_test.myCollection" --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.0 spark_submit_job.py

"""

import json
import re
import sys
import time
import dateutil.parser as du

from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.functions import explode, udf
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, ArrayType


# building Spark session and SQLcontext
spark = SparkSession.builder.appName("Preprocessing App").getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)


"""
This function is called by the __main__ function after extracting the necessary fields from the full SQL table. 
It processes the date of the tweet by generating a string that contains the date and the time with the accuracy of 1 hour.

Returns:
    current_tweet_time -- string that contains the date and the time with the accuracy of 1 hour (minutes turned to '00')
"""
def get_tweet_time(date):
    # parsing with fancy method
    now = du.parse(date)
    # Keep tweet occurances at 1-hour intervals
    current_tweet_time = str(now.year) + "-" + str(now.month) + "-" + str(now.day) + " " + str(now.hour) + ":00"
    return current_tweet_time

# register method with Spark instance
tweet_time = udf(lambda z: get_tweet_time(z))
spark.udf.register("tweet_time", tweet_time)


"""


Returns:
    current_tweet_time -- string that contains the date and the time with the accuracy of 1 hour (minutes turned to '00')
"""
def _get_hashtag(entities):
    return(entities.text)

get_hash_tag = udf(lambda z: _get_hashtag(z))
spark.udf.register("get_hash_tag", get_hash_tag)


if __name__ == '__main__':
    folder_data = sqlContext.read.json("/home/ubuntu/S3/Unzipped")
    folder_data.registerTempTable("tweets")

    extracted_SQL_table = sqlContext.sql("SELECT distinct id, created_at, lang, entities.hashtags FROM tweets WHERE lang = 'en' AND size(entities.hashtags) > 0")

    # create hashtags
    HashTagsTable = extracted_SQL_table.select("created_at", explode( "hashtags"))

    HashTagsTable_WithDates = HashTagsTable.withColumn('Keyword', get_hash_tag('col')).withColumn('Time', tweet_time('created_at') )

    # clean up table
    columns_to_drop = ['created_at', 'col']
    HashTagsTable_WithDates = HashTagsTable_WithDates.drop(*columns_to_drop)

    #HashTagsTable_WithDates.show()
    to_mongo = HashTagsTable_WithDates.groupBy('Keyword', 'Time').count()
    to_mongo.write.format("com.mongodb.spark.sql.DefaultSource").mode("append").option("database","all_tweets").option("collection", "full_db_compressed").save()

    spark.stop()

