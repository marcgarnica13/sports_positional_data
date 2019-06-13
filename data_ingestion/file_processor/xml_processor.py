import logging as lg
import time
import json

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import functions

from data_ingestion.file_processor.basic import Basic
from data_ingestion import utils, config


class XMLProcessor(Basic):

    def __init__(self, file_path, rowtag, value_tag, time_format):
        super().__init__()
        self.exploded_columns = []
        self.exploded_columns_alias = []
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

    def append_select_columns(self, column, column_alias, nested):
        if '/' not in column:
            super().append_select_columns(column, column_alias, nested)
        else:
            col_name, attribute = self._explode_dataframe_structure(column)
            super().append_select_columns("{}.{}".format(col_name, attribute), column_alias, nested)

    def append_array_columns(self, array_column, array_column_alias, nested):
        if '/' not in array_column:
            super().append_select_columns(array_column, array_column_alias, nested)
        else:
            col_name, attribute = self._explode_dataframe_structure(array_column)
            super().append_array_columns("{}.{}".format(col_name, attribute), array_column_alias, nested)

    def set_partition(self, interval, field):
        processed_field, alias = self._explode_dataframe_structure(field)
        print(processed_field, alias)
        super().set_partition(interval, "{}.{}".format(processed_field, alias))

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
                if '.' in col_name:
                    column_split = col_name.split('.')
                    self.df = self.df.select('*',
                                         functions.explode(functions.col(col_name)).alias(alias))
                    field_names = ["{}_temp.{}".format(column_split[0],s) for s in self.df.schema[column_split[0]].dataType.names if s not in [column_split[1]]]
                    self.df = self.df.withColumnRenamed(column_split[0], column_split[0] + '_temp').\
                        withColumn(column_split[0], functions.struct(field_names)).drop(column_split[0] + '_temp')
                else:
                    self.df = self.df.select('*',
                                         functions.explode(functions.col(col_name)).alias(alias)).drop(col_name)
                lg.debug("{0} array explode done in {1} seconds".format(col_name, time.time() - start))
                self.exploded_columns.append(col_name)

        if not col_name.startswith('_'):
            col_name = "_{}".format(col_name)

        return alias, split[-1]

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
                data = data.withColumn('schema_identifier', functions.concat(functions.col('schema_identifier'), functions.lit("#{}{}_{}".format(self.time_interval, self.time_units, iteration))))

            return data.toJSON().collect()