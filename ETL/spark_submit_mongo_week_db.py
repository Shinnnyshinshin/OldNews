"""
Notes : 
This is how the database connection is going to be initialized
pyspark --conf "spark.mongodb.input.uri=mongodb://ec2-54-184-196-78.us-west-2.compute.amazonaws.com/all_tweets.test_one_day"               --conf "spark.mongodb.output.uri=mongodb://ec2-54-184-196-78.us-west-2.compute.amazonaws.com/all_tweets.test_one_day" --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.0

This is how this script is run using a spark submit job
spark-submit --conf "spark.mongodb.input.uri=mongodb://ec2-54-184-196-78.us-west-2.compute.amazonaws.com/all_tweets.test_one_day"               --conf "spark.mongodb.output.uri=mongodb://ec2-54-184-196-78.us-west-2.compute.amazonaws.com/all_tweets.test_one_day" --packages org.mongodb.spark:mongo-spark-connector_2.11:2.4.0 spark_submit_mongo.py
"""

import json
import re
import sys
import time
import dateutil.parser as du
from pymongo import MongoClient
from pyspark.sql import functions as sf
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.functions import explode, udf, collect_list, struct, split
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, ArrayType

# build 
spark = SparkSession.builder.appName("Preprocessing App").getOrCreate()
sc = spark.sparkContext
sqlContext = SQLContext(sc)

"""
User Defined function to extract tweet times
"""
def get_tweet_time(s):
    # parsing with dateutil parser
    now = du.parse(s)
    # Keep tweet occurances at 1-hour intervals
    current_tweet_time = str(now.year) + "-" + str(now.month) + "-" + str(now.day) + " " + str(now.hour) + ":00"
    return current_tweet_time

# register method
tweet_time = udf(lambda z: get_tweet_time(z))
spark.udf.register("tweet_time", tweet_time)


"""
Extract just the Hashtags as entities
"""
def _get_hashtag(entities):
    return(entities.text)

# register method
get_hash_tag = udf(lambda z: _get_hashtag(z))
spark.udf.register("get_hash_tag", get_hash_tag)


def transform_Data(extracted_sql_table):
    HashTagsTable = extracted_sql_table.select("created_at", explode( "hashtags"))
    HashTagsTable_WithDates = HashTagsTable.withColumn('Keyword', get_hash_tag('col')).withColumn('Time', tweet_time('created_at') )
    
    split_col = split(HashTagsTable_WithDates['Time'], '\s')
    HashTagsTable_WithDates = HashTagsTable_WithDates.withColumn('DayOnly', split_col.getItem(0))
    HashTagsTable_WithDates = HashTagsTable_WithDates.withColumn('FullKeyword', sf.concat(sf.col('Keyword'), sf.lit('-----'), sf.col('DayOnly')))

    
    # clean up table
    columns_to_drop = ['created_at', 'col']
    hashtags_table = HashTagsTable_WithDates.drop(*columns_to_drop)
    before_aggregation = hashtags_table.groupBy('Keyword', 'Time').count() 
    after_aggregation = (before_aggregation.groupBy("Keyword").agg(collect_list(struct("Time", "count")).alias('occurances')))
    return(after_aggregation)
    
    
    

if __name__ == '__main__':
    with open('test_hashtagoldnews_config.json', 'r') as config_file:
        config_dict = json.load(config_file)

    folder_data = sqlContext.read.json(config_dict['S3_root'] + "/OneWeek")
    folder_data.registerTempTable("tweets")

    extracted_SQL_table = sqlContext.sql("SELECT distinct id, created_at, lang, entities.hashtags FROM tweets WHERE lang = 'en' AND size(entities.hashtags) > 0")


folder_data = sqlContext.read.json(config_dict['S3_root'] + "/Unzipped")
    folder_data.registerTempTable("tweets")
    client = MongoClient(config_dict['mongo_path'])
    
    db = client.all_tweets
    extracted_SQL_table = sqlContext.sql("SELECT distinct id, created_at, lang, entities.hashtags FROM tweets WHERE lang = 'en' AND size(entities.hashtags) > 0")
    after_aggregation = transform_Data(extracted_SQL_table)
    
    client = MongoClient(config_dict['mongo_path'])
    db = client.all_tweets
    # setting TTL
    db.week_db_uncompressed.ensure_index("Keyword", expireAfterSeconds=60*60*24*7)
    for row in after_aggregation.rdd.collect():
        db.week_db_uncompressed.update( { "Keyword" : row.Keyword }, { "$push" : { "occurance" : row.occurances}},upsert=True)

    spark.stop()
