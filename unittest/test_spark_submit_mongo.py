"""
test_spark_submit_mongo.py
~~~~~~~~~~~~~~
This module contains unit tests for the transformation steps of the ETL
job defined in spark_submit_mongo.py. It makes use of a local version of PySpark
that is bundled with the PySpark package.
"""
import unittest
import json
import sys
from pyspark.sql.functions import mean
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.functions import explode, udf, collect_list, struct
from pyspark.sql.types import StructField, StructType, IntegerType, StringType, ArrayType


# build
spark = SparkSession.builder.appName("Preprocessing App").getOrCreate()

# making sure the module is imported to both the python context and spark context
sys.path.append('/home/ubuntu/jobs')
import spark_submit_mongo as sp
spark.sparkContext.addPyFile('/home/ubuntu/ETL/spark_submit_mongo.py')

class TestSpark(unittest.TestCase):
    def setUp(self):
        """Start Spark, define config and path to test data
        """
        self.spark = spark
        self.sc = spark.sparkContext
        self.sqlContext = SQLContext(self.sc)
        self.config = json.loads("""{"S3_root": "/home/ubuntu/unittest/S3"}""")
       
    def tearDown(self):
        """Stop Spark
        """
        self.spark.stop()

    def test_transform(self):
        expected_data = (self.spark.read.parquet('/home/ubuntu/unittest/report'))
        expected_rows = expected_data.count()
        # test
        folder_data = self.sqlContext.read.json(self.config['S3_root'] + "/Unzipped")
        folder_data.registerTempTable("tweets")
        extracted_SQL_table = self.sqlContext.sql("SELECT distinct id, created_at, lang, entities.hashtags FROM tweets WHERE lang = 'en' AND size(entities.hashtags) > 0")
        result = sp.transform_Data(extracted_SQL_table)
        self.assertEqual(result.count(), expected_rows)

if __name__ == '__main__':
    unittest.main()
