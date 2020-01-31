from __future__ import print_function
import os
import sys
import json
from pyspark.sql import SparkSession
from dateutil.parser import *
from tokenizer import tokenizer
from collections import OrderedDict



def check_correct_fields_exist(tweet):
    if u"created_at" in tweet.keys():
        if tweet[u"lang"] == "en":
            return True
    else:
        return False


def get_tweet_time(tweet):
    timestamp = tweet[u"created_at"]
    return timestamp

if __name__ == "__main__":
    all_tweet_tokens = {}

    if len(sys.argv) != 2:
        print("Usage: s3_to_db <file>", file=sys.stderr)
        sys.exit(-1)

    spark = SparkSession\
        .builder\
        .appName("S3_to_db")\
        .getOrCreate()
    # fancy tokenizer
    T = tokenizer.TweetTokenizer(preserve_handles = False, preserve_case = False, preserve_url = False)
    # reading single file (may need to read folder as RDD)
    # https://stackoverflow.com/questions/24029873/how-to-read-multiple-text-files-into-a-single-rdd
    #lines = spark.read.json(sys.argv[1]).rdd.map(lambda r: r[0])
    #direct = "/home/ubuntu/S3Alias/test_folder/"
    for filename in os.listdir(sys.argv[1]):
        full_file_name = sys.argv[1] + filename
        tweets = []
        print("processing: " + filename)
        for line in open(full_file_name, 'r'):
            tweets.append(json.loads(line))
            # should I turn this into a lamda function? how to do it?
        for tweet in tweets:
            if check_correct_fields_exist(tweet):
                current_tweet_time = get_tweet_time(tweet)
                tweet_tokens = T.tokenize(tweet[u"text"])
                for tok in tweet_tokens:
                    if "#" in tok:
                    # efficient way to do this :
                    # https://stackoverflow.com/questions/16125229/last-key-in-python-dictionary
                        tok = tok[1:]
                        if tok not in all_tweet_tokens.keys():
                            all_tweet_tokens[tok] = {"Time":[current_tweet_time]}
                        else:
                            all_tweet_tokens[tok]["Time"].append(current_tweet_time)

    for key in all_tweet_tokens.keys():
        # adding it to
        from pymongo import MongoClient
        client = MongoClient('mongodb://ec2-54-184-196-78.us-west-2.compute.amazonaws.com:27017')
        db = client.tweet_db
        mycol = db["Keywords"]
        mydict = {"Keyword": key, "Time": all_tweet_tokens[key]["Time"]}
        x = mycol.insert_one(mydict)

    spark.stop()
