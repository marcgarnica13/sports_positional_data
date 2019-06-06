import logging as lg
import time

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions, types

from data_ingestion import utils
from data_ingestion.file_processor.basic import Basic

def epoch_to_datetime(x):
    return time.localtime(x)


class CSVProcessor(Basic):

    def __init__(self, file_path, header, delimiter):
        super().__init__()
        conf = SparkConf()
        conf.set('spark.logConf', 'true')
        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()
        self.spark.sparkContext.setLogLevel("OFF")
        self.df = self.spark.read.csv(
            file_path,
            header=header,
            sep=delimiter,
            inferSchema=True).cache()
        print("CSV Init")


    def process(self):
        c, ca, l, la, a, aa, nested_name = self.copy_processor_arrays()

        if len(c) + len(l) != 0:
            if len(a) == 0:
                lg.debug(
                    "Running select operation on pyspark dataframe with select attributes {0}, select literals {1}".format(
                        c, l))

                return self.df.select(
                    [functions.col(c).alias(ca[i]) for i, c in enumerate(c)] + [
                        functions.lit(m).alias(la[i]) for i, m in enumerate(l)]) \
                    .distinct().toJSON().collect()


            else:
                lg.debug(
                    "Running select operation on pyspark dataframe with select attributes {0}, select literals {1}, group by {0}, aggregating fields {2}".format(
                        c, l, a))

                return self.df.select(
                    [functions.col(c).alias(ca[i]) for i, c in enumerate(c)] +
                    [functions.lit(c).alias(la[i]) for i, c in enumerate(l)] +
                    [functions.col(c).alias(aa[i]) for i, c in enumerate(a)]
                ) \
                    .groupBy(
                    [functions.col(c) for c in ca] +
                    [functions.col(m) for m in la]
                ) \
                    .agg(
                    functions.collect_set(functions.struct(*[c for c in (aa)])).alias(
                        self.nested_array_name)
                ).toJSON().map(
                    lambda row: utils.add_array_index(row, index_list=aa, array_name=nested_name)).collect()

