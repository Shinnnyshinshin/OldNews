from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

spark_context = spark.sparkContext
sql_context = SQLContext(spark_context)

def extract_essential_fields(df):
    return df.select(col('created_at'),col('text'),col('lang'))

if __name__ == '__main__':
    # read entire folder
	folder_data = sqlcontext.read.json("/home/ubuntu/S3/Unzipped")
	folder_data = extract_essential_fields(folder_data)
    # create parquet file with partitioning by 'created_at' field
    folder_data.write.format("parquet").partitionBy("created_at").option("path", "/home/ubuntu/testparquet").saveAsTable("tweets")
	spark.stop()
