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

def newObject(self, collection, value, row, id):
    obj = {}
    for nested_key, nested_value in value.items():
        if (nested_key not in ['a', 'additional_fields']):
            if check_user_input(nested_value):
                self.validation_messages.append(create_new_message('inputText', nested_key, nested_key, 'Specify ' + nested_key + ' for object ' + str(id) + ' in ' + collection + '.'))
            else:
                obj[nested_key] = row.get(nested_value) if not nested_value.startswith('dshs#mapping_') else nested_value[len('dshs#mapping_'):]
                if obj[nested_key] is None:
                    self.validation_messages.append(create_new_message('alert-danger', '', '', 'Data does not include <<' + nested_value + '>> to get ' + nested_key + ' of object ' + str(id) + ' in ' + collection))
        elif nested_key == 'additional_fields':
            for additional_column in nested_value:
                obj[utils.urlify(additional_column)] = row.get(additional_column)
                if obj[utils.urlify(additional_column)] is None:
                    self.validation_messages.append(create_new_message('alert-warning', '', '', 'Data does not include <<' + additional_column + '>> of object ' + str(id) + ' in ' + collection))
    return obj



class DataImport():

    def __init__(self, mapping, name, message, file_name):
        self.mapping_id = mapping
        self.author = name
        self.import_message = message
        self.data_file_name = file_name
        self.m = utils.get_document_by_id('mappings', self.mapping_id)
        
    def validate(self):
        ds_validate = self.validate_datasource()
        db_validate = self.validate_database()
        return (ds_validate and db_validate), self.validation_messages

    def validate_datasource(self):
        self.ImportFile = {}
        self.ListOfIds = {}

        self.validation_messages = []
        self.validation_messages.append(create_new_message('text', '', '', 'Import author: ' + self.author))
        self.validation_messages.append(create_new_message('text', '', '', 'Import message: ' + self.import_message))
        self.validation_messages.append(create_new_message('text', '', '', 'Import data file: ' + self.data_file_name))
        self.validation_messages.append(create_new_message('text', '', '', 'Data file mapping: ' + self.m['name']))
        self.validation_messages.append(create_new_message('text', '', '', 'Data file format: ' + self.m['file']['format']))

        if utils.check_folder(os.path.join('temp', self.data_file_name), False):
            self.data = pd.read_csv(os.path.join('temp', self.data_file_name), delimiter=self.m['file']['delimiter'], header=self.m['file']['header'])
            data_points = self.m['data_point']

            for k,v in data_points.items():
                if (v['a'] == 'feature'):
                    feature_value = v['value']
                    if (check_user_input(feature_value)):
                        self.validation_messages.append(create_new_message('inputText', k, k, v['description']))
                    else:
                        self.ImportFile[k] = (v['value'])
                elif (v['a'] == 'collection'):
                    for i, row in self.data.iterrows():
                        if (k not in self.ImportFile.keys()):
                            self.ImportFile[k] = []
                            self.ListOfIds[k] = []
                        object_id_col = v['schema_identifier']
                        if (check_user_input(object_id_col)):
                            self.validation_messages.append(create_new_message('inputText', k, k, k + ' schema identifier'))
                        else:
                            object_id = row[object_id_col] if not object_id_col.startswith('dshs#mapping_') else object_id_col[len('dshs#mapping_'):]
                            if (object_id not in self.ListOfIds[k]):
                                self.ListOfIds[k].append(object_id)
                                self.ImportFile[k].append(newObject(self,k,v,row, object_id))

            pp = pprint.PrettyPrinter(indent=4)
            pp.pprint(self.ImportFile)
            pp.pprint(self.ListOfIds)

            print('new Import validated')
            return True

        else:
            return False
        
    def validate_database(self):
        return True
        


