import pandas as pd
import os
import requests
import pprint
import json

from data_ingestion import config

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
    obj['schema_identifier'] = str(id)
    for nested_key, nested_value in value.items():
        if (nested_key not in ['a', 'schema_identifier','additional_fields']):
            if check_user_input(nested_value):
                if len(self.ListOfIds[collection])== 0:
                    self.validation_messages['DS'].append(create_new_message('inputText', nested_key, nested_key, 'Specify ' + nested_key + ' for object ' + str(id) + ' in ' + collection + '.'))
            else:
                obj[nested_key] = row.get(nested_value) if not nested_value.startswith('dshs#mapping_') else nested_value[len('dshs#mapping_'):]
                if obj[nested_key] is None:
                    self.validation_messages['DS'].append(create_new_message('alert-danger', '', '', 'Data does not include <<' + nested_value + '>> to get ' + nested_key + ' of object ' + str(id) + ' in ' + collection))
        elif nested_key == 'additional_fields':
            for additional_column in nested_value:
                obj[utils.urlify(additional_column)] = row.get(additional_column)
                if obj[utils.urlify(additional_column)] is None:
                    self.validation_messages['DS'].append(create_new_message('alert-warning', '', '', 'Data does not include <<' + additional_column + '>> of object ' + str(id) + ' in ' + collection))
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

        self.validation_messages = {}
        self.validation_messages['DS'] = []
        self.validation_messages['DS'].append(create_new_message('text', '', '', 'Import author: ' + self.author))
        self.validation_messages['DS'].append(create_new_message('text', '', '', 'Import message: ' + self.import_message))
        self.validation_messages['DS'].append(create_new_message('text', '', '', 'Import data file: ' + self.data_file_name))
        self.validation_messages['DS'].append(create_new_message('text', '', '', 'Data file mapping: ' + self.m['name']))
        self.validation_messages['DS'].append(create_new_message('text', '', '', 'Data file format: ' + self.m['file']['format']))

        if utils.check_folder(os.path.join('temp', self.data_file_name), False):
            self.data = pd.read_csv(os.path.join('temp', self.data_file_name), delimiter=self.m['file']['delimiter'], header=self.m['file']['header'])
            data_points = self.m['data_point']

            for k,v in data_points.items():
                if (v['a'] == 'feature'):
                    feature_value = v['value']
                    if (check_user_input(feature_value)):
                        self.validation_messages['DS'].append(create_new_message('inputText', k, k, v['description']))
                    else:
                        self.ImportFile[k] = (v['value'])
                elif (v['a'] == 'collection'):
                    input_asked = False
                    for i, row in self.data.iterrows():
                        if (k not in self.ImportFile.keys()):
                            self.ImportFile[k] = []
                            self.ListOfIds[k] = []
                        object_id_col = v['schema_identifier']
                        if (check_user_input(object_id_col) and not input_asked):
                            input_asked = True
                            self.validation_messages['DS'].append(create_new_message('inputText', k + '#schema_identifier', k + '#schema_identifier', k + ' schema identifier'))
                        elif not input_asked:
                            object_id = str(row[object_id_col]) if not object_id_col.startswith('dshs#mapping_') else object_id_col[len('dshs#mapping_'):]
                            if ((object_id, None, True) not in self.ListOfIds[k]):
                                self.ListOfIds[k].append((object_id, None, True))
                                self.ImportFile[k].append(newObject(self,k,v,row, object_id))

            pp = pprint.PrettyPrinter(indent=4)
            pp.pprint(self.ImportFile)
            pp.pprint(self.ListOfIds)

            return True

        else:
            return False

    def check_upsert(self, collection, id_list):
        new_items = 0
        update_items = 0
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        for i, (id, etag, new) in enumerate(id_list):
            print(i)
            api_url = '{0}{1}/{2}'.format(config.MONGODB_API_URL,collection, id)
            print((api_url))
            response = requests.get(api_url, headers=headers)
            print(response.content)
            jsonResponse = json.loads(response.content)
            if (response.status_code != 200):
                new_items += 1
            else:
                self.ListOfIds[collection][i] = (jsonResponse['_id'], jsonResponse['_etag'], False)
                update_items +=1

        return new_items, update_items
        
    def validate_database(self):
        self.validation_messages['DB'] = []
        for key in ['Teams', 'Participants', 'Games', 'GameSections']:
            new_items, update_items = self.check_upsert(key, self.ListOfIds[key])
            self.validation_messages['DB'].append(create_new_message('text', '', '', '{} CREATE operations in collection {}.'.format(new_items, key)))
            self.validation_messages['DB'].append(create_new_message('text', '', '', '{} UPDATE operations in collection {}.'.format(update_items, key)))
        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(self.ListOfIds)
        return True

    def run(self):
        list_of_response = ""
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        for key in ['Teams', 'Participants', 'Games', 'GameSections']:
            bulk_inserts = []
            print(key)
            for i, value in enumerate(self.ImportFile[key]):
                id, etag, new = self.ListOfIds[key][i]
                if not new:
                    etag_headers = headers
                    etag_headers['If-Match'] = etag
                    api_url = '{0}{1}/{2}'.format(config.MONGODB_API_URL, key, id)
                    self.response = requests.patch(api_url, headers=etag_headers, data=json.dumps(value))
                    print(self.response.content)
                    list_of_response += str(self.response.status_code)
                else:
                    bulk_inserts.append(value)


            if len(bulk_inserts) != 0:
                print('Creating new ' + key)
                api_url = '{0}{1}'.format(config.MONGODB_API_URL, key)
                self.response = requests.post(api_url, headers=headers, data=json.dumps(bulk_inserts))
                print(self.response.content)
                list_of_response += str(self.response.status_code)

        return list_of_response




