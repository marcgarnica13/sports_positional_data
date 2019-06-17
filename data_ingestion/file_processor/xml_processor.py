import logging as lg
import time
import json

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import functions

from data_ingestion.file_processor.nested_processor import NestedProcessor
from data_ingestion import utils, config


class XMLProcessor(NestedProcessor):

    def __init__(self, file_path, rowtag, value_tag, time_format):
        super().__init__()
        self.time_format = time_format
        self.spark = SparkSession.builder.config(conf=config.SPARK_CONF).getOrCreate()
        self.spark.sparkContext.setLogLevel("OFF")
        self.df = self.spark.read.format('xml').\
            options(rowtag=rowtag). \
            options(valuetag=value_tag). \
            load(file_path).cache()
        lg.debug(self.df.schema.json())
        self.metadata = utils.process_schema(json.loads(self.df.schema.json()))
        lg.debug(json.dumps(self.metadata))