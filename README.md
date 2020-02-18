# Motivation

The motivation between #OldNews began with this question.  How can businesses prevent releasing a product or an advertisement that will hit the market too late? 

Of course, there are an infinite number of ways this question can be tackled. Entire industries exist to measure the increase or decrease of trends. How can a trend even be measured? Is it the most popular hashtag on Instagram, or the most liked post?  Is it the most viewed video on Youtube, or the one that was the most shared?

It would be foolish to try and tackle all of these things at once, but I sought to seek a common thread.  A realization I eventually had was that effectively analyzing trends depended upon effectively analyzing different time scales.  A trend can seem to be going up in the last day or week when it has in fact been decreasing for months.  Or a trend can come back in multi-year cycles and what was old can become new again (ie Stranger Things and the resurgence of 1980s culture). 

From this through process, the goal #OldNews became to build a database architecture that could effectively store and query multiple years or even decades worth of data.  

# Architecture and Implementation

### Inspiration

Many of the concepts for HashTagOldNews were inspired by the methods used by Netflix and their viewer data.  The full implementation of Netflixes' methods are largely beyond the scope of  4-week project, but I tried to include as many of the concepts as possible. 

- [https://netflixtechblog.com/scaling-time-series-data-storage-part-i-ec2b6d44ba39](https://netflixtechblog.com/scaling-time-series-data-storage-part-i-ec2b6d44ba39)
- [https://netflixtechblog.com/scaling-time-series-data-storage-part-ii-d67939655586](https://netflixtechblog.com/scaling-time-series-data-storage-part-ii-d67939655586)

### The Dataset

#OldNews in its current iteration uses 2 months of Twitter archive data from ([https://archive.org/details/twitterstream](https://archive.org/details/twitterstream)). The data is decompressed from the original bz2.tar format and consolidated using a python script called Decompress_And_Combine.py found in the ETL folder. 

### Apache Spark

Apache Spark is an open-source distributed general-purpose cluster-computing framework. Spark provides an interface for programming entire clusters with implicit data parallelism and fault tolerance.

The Spark script used to do most of the processing is spark_submit_mongo.py in the ETL directory. The script speeds things up by extracting the Hashtags from the entities field of the original Twitter data rather than processing and tokenizing the tweet text itself.