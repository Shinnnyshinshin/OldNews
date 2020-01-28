
import os
import json
import pandas as pd # this works
import dateutil
from tokenizer import tokenizer
# pip install git+https://github.com/erikavaris/tokenizer.git if it doesn't work


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
                    now = parse(timestamp)
                    to_add_to_db = now.year + "/" + now.month + "/" + now.day
                    # oh right just hash tags
                    for tok in mytokens:
                        if "#" in tok:
                            if tok not in all_tweet_tokens.keys():
                                timestamp_list = [timestamp]
                            else:
                                timestamp_list = all_tweet_tokens[tok]
                                timestamp_list.append(timestamp)
                            all_tweet_tokens[tok] = timestamp_list
                            

print(len(all_tweet_tokens))
