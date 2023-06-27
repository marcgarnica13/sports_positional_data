from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import functions

from data_ingestion import utils, config

file_path = '/home/marc/grive/DSHS/Projects/UEFA/opta/EventData/GroupA/BENL.xml'

spark = SparkSession.builder.config(conf=config.SPARK_CONF).getOrCreate()
spark.sparkContext.setLogLevel("OFF")
df = spark.read.format('xml').\
    options(rowtag='Games'). \
    options(valuetag=' '). \
    load(file_path).cache()

df.printSchema()