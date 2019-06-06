import logging as lg

class Basic():

    def __init__(self):
        self.reset_cols()
        print("Basic")

    def reset_cols(self):
        self.select_cols = []
        self.select_cols_alias = []
        self.array_cols = []
        self.array_cols_alias = []
        self.select_literals = []
        self.select_literals_alias = []
        self.nested_array_name = ''

    def append_select_columns(self, column, column_alias):
        lg.debug("Adding select column {} with alias {}".format(column, column_alias))
        self.select_cols.append(column)
        self.select_cols_alias.append(column_alias)

    def append_select_literals(self, literal, literal_alias):
        lg.debug("Adding literal column {} with alias {}".format(literal, literal_alias))
        self.select_literals.append(literal)
        self.select_literals_alias.append(literal_alias)

    def set_nested_array_name(self, array_name):
        self.nested_array_name = array_name

    def append_array_columns(self, array_column, array_column_alias):
        lg.debug("Adding array column {} with alias {}".format(array_column, array_column_alias))
        self.array_cols.append(array_column)
        self.array_cols_alias.append(array_column_alias)

    def copy_processor_arrays(self):
        return list(self.select_cols), \
               list(self.select_cols_alias), \
               list(self.select_literals), \
               list(self.select_literals_alias), \
               list(self.array_cols), \
               list(self.array_cols_alias), \
               (self.nested_array_name)

    def get_processor_arrays(self):
        return self.select_cols, \
               self.select_cols_alias, \
               self.select_literals, \
               self.select_literals_alias, \
               self.array_cols, \
               self.array_cols_alias, \
               self.nested_array_name

    def set_processor_arrays(self, c, ca, l, la, a, aa):
        self.select_cols = c
        self.select_cols_alias = ca
        self.select_literals = l
        self.select_literals_alias = la
        self.array_cols = a
        self.array_cols_alias = aa
