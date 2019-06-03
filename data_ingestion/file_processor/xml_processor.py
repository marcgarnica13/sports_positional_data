import logging as lg
import time

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import functions

from data_ingestion.file_processor.basic import Basic


class XMLProcessor(Basic):

    def __init__(self, file_path, rowtag):
        super().__init__()
        self.exploded_columns = []
        self.exploded_columns_alias = []
        conf = SparkConf()
        conf.set('spark.logConf', 'true')
        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()
        self.spark.sparkContext.setLogLevel("ERROR")
        self.df = self.spark.read.format('xml').\
            options(rowtag=rowtag).\
            load(file_path).persist()

        print("XML Init")


    def pre_process(self):
        lg.debug("Accessing temporal folder to read the import data file.")
        conf = SparkConf()
        conf.set('spark.logConf', 'true')
        spark = SparkSession.builder.config(conf=conf).getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
        start = time.time()
        self.df.printSchema()

        frame_df = self.df.select('*', functions.explode("Frame").alias("Moment")).select('*', functions.col(
            "Moment._N").alias("Moment_id")). \
            groupBy("Moment_id", "Moment._T", "_GameSection", "Moment._BallStatus", "Moment._BallPossession").agg(
            functions.collect_list(
                functions.struct("_PersonId", "Moment._M", "Moment._S", "Moment._T", "Moment._X", "Moment._Y",
                                 "Moment._Z")).alias("participants")).persist()

        frame_df.printSchema()
        print(frame_df.filter(functions.col('Moment_id') == 100001).show(20, False))
        # print(frame_df.toJSON().collect())
        # print(self.df.select('Frame').show(10, False))
        print("{} seconds".format(time.time() - start))

    def append_select_columns(self, column, column_alias):
        if '/' not in column:
            super().append_select_columns(column, column_alias)
        else:
            print(column)
            split = column.split('/')
            col_name = None
            for s in split[:-1]:
                if col_name is None:
                    col_name = s
                else:
                    col_name = "exploded_{}.{}".format(col_name, s)

                print(col_name)

                if col_name not in self.exploded_columns:
                    self.df = self.df.select('*', functions.explode(functions.col(col_name)).alias("exploded_{}".format(col_name))).persist()
                    self.exploded_columns.append(col_name)

            super().append_select_columns("exploded_{}.{}".format(col_name, split[-1]), column_alias)


    def process(self):
        c, ca, l, la, a, aa, nested_name = self.copy_processor_arrays()
        ex = self.exploded_columns
        exa = self.exploded_columns_alias
        print(c)
        print(ca)
        print(l)
        print(la)
        print(a)
        print(aa)
        print(ex)
        print(exa)

        if len(c) + len(l) != 0:
            if len(a) == 0:
                lg.debug(
                    "Running select operation on pyspark dataframe with select attributes {0}, select literals {1}".format(
                        c, l))

                self.df.printSchema()


                return self.df.select(
                    [functions.col(c).alias(ca[i]) for i, c in enumerate(c)] + [
                        functions.lit(m).alias(la[i]) for i, m in enumerate(l)]) \
                    .distinct().toJSON().collect()