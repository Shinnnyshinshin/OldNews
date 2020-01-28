
import os
import json
import pandas as pd # this works
from dateutil.parser import *
from tokenizer import tokenizer
# pip install git+https://github.com/erikavaris/tokenizer.git if it doesn't work

# here are the schemas
"""
    For the dictionary all_tweet_tokens 
        key_word : { time {  { count }  } }
        

    which in the dataframe to be entered will be
    [ key word] [ time ] [ count ]

    place dataframe into the database and then go to sleep    
"""

T = tokenizer.TweetTokenizer(preserve_handles = False, preserve_case = False, preserve_url = False)

all_tweet_tokens = {}

#direct = "/home/ubuntu/S3Alias/test_folder/"
direct = "/Users/willshin/Development/OldNews_InsightProject/twitter_parsing/test_folder/"
for filename in os.listdir(direct):
    full_file_name = direct + filename
    tweets = []
    print("processing: " + filename)
    for line in open(full_file_name, 'r'):
        tweets.append(json.loads(line))
    for tweet in tweets:
        if "created_at" in tweet.keys():
            if "lang" in tweet.keys():
                if tweet["lang"] == "en":
                    mytokens = T.tokenize(tweet["text"])
                    timestamp = tweet["created_at"]
                    # parsing with fancy method
                    now = parse(timestamp)
                    current_tweet_time = str(now.year) + "/" + str(now.month) + "/" + str(now.day) + "/" + str(now.hour)
                    # oh right just hash tags
                    for tok in mytokens:
                        if "#" in tok:
                            # if it is new
                            if tok not in all_tweet_tokens.keys():
                                all_tweet_tokens[tok] = {"time":{current_tweet_time : 1}}
                            else:
                                time_for_tweet = all_tweet_tokens[tok]
                                # if it is a new time
                                if current_tweet_time not in time_for_tweet["time"].keys():
                                    time_for_tweet["time"] = {current_tweet_time : 1}
                                    all_tweet_tokens[tok] = current_tweet_time
                                        
                                    # if it is an additional count
                                else:
                                    old_count = time_for_tweet["time"][current_tweet_time]
                                    time_for_tweet["time"][current_tweet_time] = old_count + 1
                                    all_tweet_tokens[tok] = time_for_tweet
        

# turn into Pandas Dataframe

pd_df = pd.DataFrame(columns=["Word", "DateTime", "Freq"])

# this is the big loop
for word in all_tweet_tokens.keys():
    
    time_dict = all_tweet_tokens[word]["time"]
    for tweet_time in time_dict.keys():
        tweet_freq = time_dict[tweet_time]
        pd_df = pd_df.append({"Word": word, "DateTime": tweet_time, "Freq": tweet_freq}, ignore_index=True)


# mock more : the current data only does 2019/8/1/6 -- so here we are adding 7
for word in all_tweet_tokens.keys():
    time_dict = all_tweet_tokens[word]["time"]
    for tweet_time in time_dict.keys():
        tweet_freq = time_dict[tweet_time]
        tweet_time = "2019/8/1/7"
        pd_df = pd_df.append({"Word": word, "DateTime": tweet_time, "Freq": tweet_freq}, ignore_index=True)


# messi is one that we kow

# mock more : the current data only does 2019/8/1/6 -- so here we are adding 7
for word in all_tweet_tokens.keys():
    time_dict = all_tweet_tokens[word]["time"]
    for tweet_time in time_dict.keys():
        tweet_freq = time_dict[tweet_time]
        tweet_time = "2019/8/1/8"
        pd_df = pd_df.append({"Word": word, "DateTime": tweet_time, "Freq": tweet_freq}, ignore_index=True)




# turn into spark_df
# so pandas is not compatible

spark_df = spark.createDataFrame(pd_df)

# 








# now to turn this into a dataframe
# https://redislabs.com/blog/getting-started-redis-apache-spark-python/

