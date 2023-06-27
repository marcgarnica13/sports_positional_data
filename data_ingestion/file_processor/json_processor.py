import logging as lg
import time
import json
import os

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import functions

from file_processor.nested_processor import NestedProcessor
import utils, config

class JSONProcessor(NestedProcessor):

    def __init__(self, file_path, time_format):
        start = time.time()
        super().__init__()
        self.exploded_columns = []
        self.exploded_columns_alias = []
        self.time_format = time_format
        self.spark = SparkSession.builder.config(conf=config.SPARK_CONF).getOrCreate()
        self.spark.sparkContext.setLogLevel("OFF")
        job_text = "{}#Reading data file#Dataframe creation and inferring schema".format(os.path.basename(file_path))
        self.set_job_description(job_text)
        self.df = self.spark.read.json(file_path).cache()
        lg.debug(self.df.schema.json())
        self.metadata = utils.process_schema(json.loads(self.df.schema.json()))
        self.df.printSchema()

        lg.debug(json.dumps(self.metadata))
        lg.debug("JSON init function executed in {} seconds".format(time.time() - start))

