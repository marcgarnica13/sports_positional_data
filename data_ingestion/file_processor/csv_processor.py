import logging as lg
import time
import json
import os

from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions, types

from data_ingestion import utils, config
from data_ingestion.file_processor.basic import Basic

def epoch_to_datetime(x):
    return time.localtime(x)


class CSVProcessor(Basic):

    def __init__(self, file_path, header, delimiter, time_format):
        super().__init__()
        self.spark = SparkSession.builder.config(conf=config.SPARK_CONF).getOrCreate()
        self.spark.sparkContext.setLogLevel("OFF")
        job_text = "{}#Reading data file#Dataframe creation and inferring schema".format(os.path.basename(file_path))
        self.set_job_description(job_text)
        self.df = self.spark.read.csv(
            file_path,
            header=header,
            sep=delimiter,
            inferSchema=True).cache()
        self.time_format = time_format
        self.metadata = utils.process_schema(json.loads(self.df.schema.json()))
        lg.debug("CSV Metadata: {}".format(self.metadata))


    def _run_queries(self, dataframe, iteration=0):
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

            if (len(a) != 0):
                data = data.groupBy(
                    [functions.col(c) for c in ca] +
                    [functions.col(m) for m in la]
                ).agg(
                    functions.collect_list(functions.array(*([c for c in (aa)])).alias(
                        self.nested_array_name)
                    )
                )

            if len(nested_c) != 0:
                if len(nested_a) != 0:
                    nested_ca.append(nested_nested_array_name)

                data = data.groupBy(
                    [functions.col(c) for c in ca] +
                    [functions.col(m) for m in la]
                ).agg(
                    functions.collect_list(functions.struct(*([c for c in (nested_ca)]))).alias(
                        self.nested_collection_name)
                    )
            if len(a) != 0 and len(nested_a) != 0:
                data = data.withColumn(
                    "{}_cols".format(nested_array_name), functions.array(*[functions.lit(c) for c in aa])
                ).withColumn(
                    "{}_cols".format(nested_nested_array_name), functions.array(*[functions.lit(c) for c in nested_aa])
                )
            elif len(a) != 0:
                data = data.withColumn(
                    "{}_cols".format(nested_array_name), functions.array(*[functions.lit(c) for c in aa])
                )
            elif len(nested_a) != 0:
                data = data.withColumn(
                    "{}_cols".format(nested_nested_array_name), functions.array(*[functions.lit(c) for c in nested_aa])
                )

            if self.partition:
                data = data.withColumn('schema_identifier', functions.concat(functions.col('schema_identifier'), functions.lit("__{}{}_{}".format(self.time_interval, self.time_units, iteration))))

            return data.toJSON().collect()
