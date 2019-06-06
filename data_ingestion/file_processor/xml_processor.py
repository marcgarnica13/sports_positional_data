import logging as lg
import time

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import functions

from data_ingestion.file_processor.basic import Basic
from data_ingestion import utils


class XMLProcessor(Basic):

    def __init__(self, file_path, rowtag):
        super().__init__()
        self.exploded_columns = []
        self.exploded_columns_alias = []
        conf = SparkConf()
        conf.set('spark.logConf', 'true')
        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()
        self.spark.sparkContext.setLogLevel("OFF")
        self.df = self.spark.read.format('xml').\
            options(rowtag=rowtag).\
            load(file_path).cache()

    def append_select_columns(self, column, column_alias):
        if '/' not in column:
            super().append_select_columns(column, column_alias)
        else:
            col_name, attribute = self._explode_dataframe_structure(column)
            super().append_select_columns("{}.{}".format(col_name, attribute), column_alias)

    def append_array_columns(self, array_column, array_column_alias):
        if '/' not in array_column:
            super().append_select_columns(array_column, array_column_alias)
        else:
            col_name, attribute = self._explode_dataframe_structure(array_column)
            super().append_array_columns("{}.{}".format(col_name, attribute), array_column_alias)

    def _explode_dataframe_structure(self, column):
        split = column.split('/')
        col_name = None
        alias = None
        lg.debug("New split {}".format(split))
        for s in split[:-1]:
            if col_name is None:
                col_name = s
                alias = "_{}".format(s)
            else:
                col_name = "{}.{}".format(alias, s)
                alias = "_{}".format(s)


            if col_name not in self.exploded_columns:
                lg.debug("Column {0} needs to be exploded with alias {1}".format(col_name, alias))
                start = time.time()
                self.df = self.df.select('*',
                                         functions.explode(functions.col(col_name)).alias(alias)).drop(col_name)

                lg.debug("{0} array explode done in {1} seconds".format(col_name, time.time() - start))
                self.exploded_columns.append(col_name)

        if not col_name.startswith('_'):
            col_name = "_{}".format(col_name)

        return alias, split[-1]

    def process(self):
        c, ca, l, la, a, aa, nested_name = self.get_processor_arrays()
        ex = self.exploded_columns
        exa = self.exploded_columns_alias

        start = time.time()


        if len(c) + len(l) != 0:
            if len(a) == 0:
                lg.debug(
                    "Running select operation on pyspark dataframe with select attributes {0}, select literals {1}".format(
                        c, l))
                collection_data = self.df.select(
                    [functions.col(c).alias(ca[i]) for i, c in enumerate(c)] + [
                        functions.lit(m).alias(la[i]) for i, m in enumerate(l)]) \
                    .distinct().toJSON().collect()
                return collection_data
            else:
                lg.debug(
                    "Running select operation on pyspark dataframe with select attributes {0} {1}, select literals {2} {3}, group by {0} {1}, aggregating fields {4} {5}".format(
                        c, ca, l, la, a, aa))

                collection_data = self.df.\
                select(
                        [functions.col(c).alias(ca[i]) for i, c in enumerate(c)] +
                        [functions.lit(c).alias(la[i]) for i, c in enumerate(l)] +
                        [functions.col(c).alias(aa[i]) for i, c in enumerate(a)] +
                        [functions.col('_Frame._BallPossession').alias('BallPossession'), functions.col('_Frame._BallStatus').alias('BallStatus')]
                    )\
                .groupBy(
                        [functions.col(c) for c in ca] +
                        [functions.col(m) for m in la]
                    )\
                .agg(functions.sum('BallStatus').alias('BallStatus'), functions.sum('BallPossession').alias('BallPossession'), functions.collect_list(functions.array(*[c for c in (aa)])).alias(nested_name))\
                    .toJSON().map(
                    lambda row: utils.add_array_index(row, index_list=aa, array_name=nested_name)).collect()

                print("SELECT + AGGREGATE + toJSON COLLECT: {} seconds".format(time.time() - start))

                return collection_data