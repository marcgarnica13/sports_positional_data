import pandas as pd
import os
import pprint

from data_ingestion import utils

pd.set_option('display.max_columns', 500)

def create_new_message(type, id, name, message):
    newMessage = {
        'type': type,
        'id': id,
        'name': name,
        'message': message
    }
    return newMessage

def check_user_input(w):
    return w == 'dshs#input'

def check_mapping_input(w):
    if (w.startswith('dshs#mapping_')):
        return True, w[len('dshs#mapping_'):]
    else:
        return False, ""

def newObject(self, collection, value, row):
    obj = {}
    for nested_key, nested_value in value.items():
        if (nested_key not in ['a', 'additional_fields']):
            if check_user_input(nested_value):
                self.validation_messages.append(create_new_message('inputText', nested_key, nested_key, 'Specify ' + nested_key + ' for object ' + str(obj['schema_identifier']) + ' in ' + collection + '.'))
            else:
                obj[nested_key] = row.get(nested_value) if not nested_value.startswith('dshs#mapping_') else nested_value[len('dshs#mapping_'):]
                if obj[nested_key] is None:
                    self.validation_messages.append(create_new_message('alert-danger', '', '', 'Data does not include <<' + nested_value + '>> to get ' + nested_key + ' of ' + collection))
        elif nested_key == 'additional_fields':
            for additional_column in nested_value:
                obj[utils.urlify(additional_column)] = row[additional_column]
    return obj



class DataImport():

    def __init__(self, mapping, name, message, file_name):
        self.mapping_id = mapping
        self.author = name
        self.import_message = message
        self.data_file_name = file_name
        self.m = utils.get_document_by_id('mappings', self.mapping_id)

    def validate(self):
        ImportFile = {}
        ListOfIds = {}

        self.validation_messages = []
        self.validation_messages.append(create_new_message('text', '', '', 'Import author: ' + self.author))
        self.validation_messages.append(create_new_message('text', '', '', 'Import message: ' + self.import_message))
        self.validation_messages.append(create_new_message('text', '', '', 'Import data file: ' + self.data_file_name))
        self.validation_messages.append(create_new_message('text', '', '', 'Data file mapping: ' + self.m['name']))
        self.validation_messages.append(create_new_message('text', '', '', 'Data file format: ' + self.m['file']['format']))

        self.data = pd.read_csv(os.path.join('temp', self.data_file_name), delimiter=self.m['file']['delimiter'], header=self.m['file']['header'])
        data_points = self.m['data_point']

        for k,v in data_points.items():
            if (v['a'] == 'feature'):
                feature_value = v['value']
                if (check_user_input(feature_value)):
                    self.validation_messages.append(create_new_message('inputText', k, k, v['description']))
                else:
                    ImportFile[k] = (v['value'])
            elif (v['a'] == 'collection'):
                for i, row in self.data.iterrows():
                    if (k not in ImportFile.keys()):
                        ImportFile[k] = []
                        ListOfIds[k] = []
                    object_id_col = v['schema_identifier']
                    if (check_user_input(object_id_col)):
                        self.validation_messages.append(create_new_message('inputText', k, k, k + ' schema identifier'))
                    else:
                        object_id = row[object_id_col] if not object_id_col.startswith('dshs#mapping_') else object_id_col[len('dshs#mapping_'):]
                        if (object_id not in ListOfIds[k]):
                            ListOfIds[k].append(object_id)
                            ImportFile[k].append(newObject(self,k,v,row))

        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(ImportFile)
        pp.pprint(ListOfIds)

        print('new Import validated')
        return self.validation_messages

