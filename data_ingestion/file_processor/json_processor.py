import logging as lg
import time
import json

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import functions

from data_ingestion.file_processor.basic import Basic
from data_ingestion import utils, config

class JSONProcessor(Basic):

    def __init__(self, file_path, time_format):
        super().__init__()
        self.exploded_columns = []
        self.exploded_columns_alias = []
        self.time_format = time_format
        self.spark = SparkSession.builder.config(conf=config.SPARK_CONF).getOrCreate()
        self.spark.sparkContext.setLogLevel("OFF")
        self.df = self.spark.read.json(file_path).cache()
        lg.debug(self.df.schema.json())
        self.metadata = utils.process_schema(json.loads(self.df.schema.json()))
        lg.debug(json.dumps(self.metadata))
        self.df.printSchema()
