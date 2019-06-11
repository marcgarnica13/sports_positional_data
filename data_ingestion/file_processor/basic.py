import logging as lg

from pyspark.sql import functions

def get_functional_interval(interval, units):
    if units == 's':
        return interval * 1000
    elif units == 'm':
        return interval * 60 * 1000
    elif units == 'h':
        return interval * 60 * 60 * 1000

class Basic():

    def __init__(self):
        self.reset_cols()
        print("Basic")

    def reset_cols(self):
        self.select_cols = []
        self.select_cols_alias = []

        self.nested_select_cols = []
        self.nested_select_cols_alias = []

        self.array_cols = []
        self.array_cols_alias = []

        self.nested_array_cols = []
        self.nested_array_cols_alias = []

        self.select_literals = []
        self.select_literals_alias = []

        self.nested_select_literals = []
        self.nested_select_literals_alias = []

        self.nested_nested_array_name = ''
        self.nested_array_name = ''
        self.partition = False

    def append_select_columns(self, column, column_alias, nested):
        lg.debug("Adding select column {} with alias {}".format(column, column_alias))
        if nested:
            self.nested_select_cols.append(column)
            self.nested_select_cols_alias.append(column_alias)
        else:
            self.select_cols.append(column)
            self.select_cols_alias.append(column_alias)

    def append_select_literals(self, literal, literal_alias, nested):
        lg.debug("Adding literal column {} with alias {}".format(literal, literal_alias))
        if nested:
            self.nested_select_literals.append(literal)
            self.nested_select_literals_alias.append(literal_alias)
        else:
            self.select_literals.append(literal)
            self.select_literals_alias.append(literal_alias)

    def set_nested_array_name(self, array_name, nested):
        if nested:
            self.nested_nested_array_name = array_name
        else:
            self.nested_array_name = array_name

    def append_array_columns(self, array_column, array_column_alias, nested):
        lg.debug("Adding array column {} with alias {}".format(array_column, array_column_alias))
        if nested:
            self.nested_array_cols.append(array_column)
            self.nested_array_cols_alias.append(array_column_alias)
        else:
            self.array_cols.append(array_column)
            self.array_cols_alias.append(array_column_alias)

    def process(self):
        if self.partition:
            self.df.cache()
            if self.time_format != 'ms':
                self.df = self.df.withColumn("converted_timestamp_ms", functions.unix_timestamp(functions.col(self.ts_field), format=self.time_format) * 1000)
                self.ts_field = "converted_timestamp_ms"
            #self.df = self.df.withColumn('minute', functions.from_unixtime(functions.col('ts in ms'), "yyyy-MM-dd'T'HH:mm:ss.SSS").cast(types.DateType()))
            minmax = self.df.agg(functions.min(self.ts_field).alias('min'), functions.max(self.ts_field).alias('max')).collect()
            interval = get_functional_interval(self.time_interval, self.time_units)
            initial = minmax[0].min
            return_list = []
            iteration = 1

            while initial < minmax[0].max:
                print("{} --- {} interval output".format(initial, initial + interval))
                filtered_df = self.df.filter(functions.col(self.ts_field).between(initial, initial + interval - 1))
                initial = initial + interval
                return_list = return_list + self._run_queries(filtered_df, iteration)
                iteration = iteration + 1
            self.df.unpersist()

            return return_list
        else:
            return self._run_queries(self.df)

    def copy_processor_arrays(self):
        return list(self.select_cols), \
               list(self.select_cols_alias), \
               list(self.nested_select_cols), \
               list(self.nested_select_cols_alias), \
               list(self.select_literals), \
               list(self.select_literals_alias), \
               list(self.nested_select_literals), \
               list(self.nested_select_literals_alias), \
               list(self.array_cols), \
               list(self.array_cols_alias), \
               (self.nested_array_name),\
               list(self.nested_array_cols), \
               list(self.nested_array_cols_alias), \
               (self.nested_nested_array_name)

    def get_processor_arrays(self):
        return self.select_cols, \
               self.select_cols_alias, \
               self.nested_select_cols, \
               self.nested_select_cols_alias, \
               self.select_literals, \
               self.select_literals_alias, \
               self.nested_select_literals, \
               self.nested_select_literals_alias, \
               self.array_cols, \
               self.array_cols_alias, \
               self.nested_array_name,\
               self.nested_array_cols, \
               self.nested_array_cols_alias, \
               (self.nested_nested_array_name)

    def set_partition(self, interval, field):
        self.partition = True
        self.ts_field = field
        self.time_units = interval[-1]
        self.time_interval = int(interval[:-1])
