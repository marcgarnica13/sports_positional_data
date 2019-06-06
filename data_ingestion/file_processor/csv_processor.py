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

        if self.partition:
            #self.df = self.df.withColumn('minute', functions.from_unixtime(functions.col('ts in ms'), "yyyy-MM-dd'T'HH:mm:ss.SSS").cast(types.DateType()))
            minmax = self.df.agg(functions.min('ts in ms').alias('min'), functions.max('ts in ms').alias('max')).collect()
            print(minmax[0])
            interval = .5*60*1000
            initial = minmax[0].min
            print(self.df.count())

            while initial < minmax[0].max:
                lg.debug("{} interval output".format(initial))
                filtered_df = self.df.filter(functions.col('ts in ms').between(initial, initial + interval - 1))
                initial = initial + interval
                lg.debug(self._run_queries(filtered_df))
        else:
            return self._run_queries(self.df)



    def _run_queries(self, dataframe):
        c, ca, nested_c, nested_ca, l, la, nested_l, nested_la, a, aa, nested_array_name, nested_a, nested_aa, nested_nested_array_name = self.copy_processor_arrays()

        if len(c) + len(l) + len(nested_c) + len(nested_l) != 0:
            lg.debug(
                "Running select operation on pyspark dataframe with select attributes {0}, select literals {1}".format(
                    c, l))

            data = dataframe.select(
                [functions.col(c).alias(ca[i]) for i, c in enumerate(c)] +
                [functions.lit(m).alias(la[i]) for i, m in enumerate(l)] +
                [functions.col(c).alias(nested_ca[i]) for i, c in enumerate(nested_c)] +
                [functions.lit(m).alias(nested_la[i]) for i, m in enumerate(nested_l)] +
                [functions.col(c).alias(aa[i]) for i, c in enumerate(a)] +
                [functions.col(c).alias(nested_aa[i]) for i, c in enumerate(nested_a)]
            ).distinct()
            data.show()
            if len(nested_a) != 0:
                data = data.groupBy(
                    [functions.col(c) for c in ca] +
                    [functions.col(m) for m in la] +
                    [functions.col(n_c) for n_c in nested_ca] +
                    [functions.col(n_m) for n_m in nested_la]
                ).agg(
                    functions.collect_list(functions.array(*([c for c in (nested_aa)]))).alias(
                        nested_nested_array_name)
                    )

            data.show()
            if (len(a) != 0):
                data = data.groupBy(
                    [functions.col(c) for c in ca] +
                    [functions.col(m) for m in la]
                ).agg(
                    functions.collect_list(functions.array(*([c for c in (aa)])).alias(
                        self.nested_array_name)
                    )
                )

            data.show()
            if len(nested_c) != 0:
                if len(nested_a) != 0:
                    nested_ca.append(nested_nested_array_name)

                data = data.groupBy(
                    [functions.col(c) for c in ca] +
                    [functions.col(m) for m in la]
                ).agg(
                    functions.collect_list(functions.struct(*([c for c in (nested_ca)]))).alias(
                        'moments')
                    )
            if len(a) != 0 and len(nested_a) != 0:
                data = data.toJSON().\
                    map(lambda row: utils.add_array_index(row, index_list=aa, array_name=nested_array_name)).\
                    map(lambda row: utils.add_array_index(row, index_list=nested_aa, array_name=nested_nested_array_name))
            elif len(a) != 0:
                data = data.toJSON(). \
                    map(lambda row: utils.add_array_index(row, index_list=aa, array_name=nested_array_name))
            elif len(nested_a) != 0:
                data = data.toJSON(). \
                    map(lambda row: utils.add_array_index(row, index_list=nested_aa, array_name=nested_nested_array_name))

            print(data.collect())
