
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

direct = "/home/ubuntu/S3Alias/test_folder/"
#direct = "/Users/willshin/Development/OldNews_InsightProject/twitter_parsing/test_folder/"
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
                    current_tweet_time = str(now.year) + "/" + str(now.month) + "/" + str(now.day)
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
        
 