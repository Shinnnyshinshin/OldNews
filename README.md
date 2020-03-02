# Motivation

The motivation between #OldNews began with this question.  How can businesses prevent releasing a product or an advertisement that will hit the market too late?

Of course, there are an infinite number of ways this question can be tackled. Entire industries exist to measure the increase or decrease of trends. How can a trend even be measured? Is it the most popular hashtag on Instagram, or the most liked post?  Is it the most viewed video on Youtube, or the one that was the most shared?

It would be foolish to try and tackle all of these things at once, but I sought to seek a common thread.  A realization I eventually had was that effectively analyzing trends depended upon effectively analyzing different time scales.  A trend can seem to be going up in the last day or week when it has in fact been decreasing for months.  Or a trend can come back in multi-year cycles and what was old can become new again (ie Stranger Things and the resurgence of 1980s culture). 

From this through process, the goal #OldNews became to build a database architecture that could effectively store and query multiple years or even decades worth of data.  

# Architecture and Implementation

### Inspiration

Many of the concepts for #OldNews were inspired by the methods used by Netflix and their viewer data.  The full implementation of Netflixes' methods are largely beyond the scope of  4-week project, but I tried to include as many of the concepts as possible.

- [https://netflixtechblog.com/scaling-time-series-data-storage-part-i-ec2b6d44ba39](https://netflixtechblog.com/scaling-time-series-data-storage-part-i-ec2b6d44ba39)
- [https://netflixtechblog.com/scaling-time-series-data-storage-part-ii-d67939655586](https://netflixtechblog.com/scaling-time-series-data-storage-part-ii-d67939655586)

### The Dataset

#OldNews in its current iteration uses 2 months of Twitter archive data from ([https://archive.org/details/twitterstream](https://archive.org/details/twitterstream)). The data is decompressed from the original bz2.tar format and consolidated using a python script called Decompress_And_Combine.py found in the ETL folder.

### Apache Spark

Apache Spark is an open-source distributed general-purpose cluster-computing framework. Spark provides an interface for programming entire clusters with implicit data parallelism and fault tolerance.

The Spark script used to do most of the processing is spark_submit_mongo.py in the ETL directory. The script speeds things up by extracting the Hashtags from the entities field of the original Twitter data rather than processing and tokenizing the tweet text itself.


#### Outline of Code

* First the JSON files are read into the Spark context using the read.json() command.
* Only the fields that describe the hashtag (in a separate field from the tweet itself), the language of the tweet and the 'created_at' field are extracted from each tweet. At this point, each entry will look like this:

```
+-------------------+--------------------+----+-----------------------+
|                 id|          created_at|lang|               hashtags|
+-------------------+--------------------+----+-----------------------+
|1164062385805692929|Wed Aug 21 06:31:...|  en|    [[[0, 8], FibDems]]|
|1164062394185854976|Wed Aug 21 06:31:...|  en|   [[[64, 69], rain]...|
|1164063061071712256|Wed Aug 21 06:33:...|  en|   [[[88, 110], Huma...|
|1164063342069276672|Wed Aug 21 06:35:...|  en|   [[[46, 67], Stopk...|
|1164063551788441600|Wed Aug 21 06:35:...|  en|   [[[60, 75], 48MPA...|
....
```

* Next we use the 'explode' function so that each hashtag gets its own entry in the table. You'll notice there are two entries for the hashtag #rain on Aug 21st. That's great. This is what #OldNews is eventually going to count. 

```
+--------------------+--------------------+
|          created_at|                 col|
+--------------------+--------------------+
|Wed Aug 21 06:31:...|   [[0, 8], FibDems]|
|Wed Aug 21 06:31:...|    [[64, 69], rain]|
|Wed Aug 21 06:31:...|    [[70, 75], rain]|
|Wed Aug 21 06:31:...|   [[76, 82], radar]|
|Wed Aug 21 06:33:...|[[88, 110], Human...|
|Wed Aug 21 06:33:...|[[112, 121], Poac...|
|Wed Aug 21 06:35:...|[[46, 67], Stopki...|
|Wed Aug 21 06:35:...|[[60, 75], 48MPAn...|
|Wed Aug 21 06:35:...|    [[76, 81], MiA3]|
```

* But before we do that, we first clean up our table by dropping columns we don't want, and format the created_at field so it considers all the occurances of hashtags that occur during each hour. 

```
>> > HashTagsTable_WithDates.show(10)
+--------------------+--------------+
|             Keyword | Time|
+--------------------+--------------+
|             FibDems | 2019-8-21 6: 00|
|                rain | 2019-8-21 6: 00|
|                rain | 2019-8-21 6: 00|
|               radar | 2019-8-21 6: 00|
```

* Finally we do a groupBy() command to count the tweets that occur in each hour-long time block. This is what will be added to the mongo database. 


## Mongo Databases

