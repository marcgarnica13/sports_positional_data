import logging as lg
import pandas as pd
import os
import requests
import pprint
import json

from data_ingestion import config

from data_ingestion import utils

pd.set_option('display.max_columns', 500)

def create_new_message(type, id, name, message):
    lg.debug("New message added {0} {1}".format(id, message))
    return {
        'type': type,
        'id': id,
        'name': name,
        'message': message
    }

def check_user_input(w):
    return w == 'dshs#input'

def check_mapping_input(w):

    if (w.startswith('dshs#mapping_')):
        return True, w[len('dshs#mapping_'):]
    else:
        return False, ""

class DataImport():

    def __init__(self, mapping, name, message, file_name):
        lg.info("New data import: {0}-{1}-{2}-{3}".format(mapping, name, message, file_name))
        self.mapping_id = mapping
        self.author = name
        self.import_message = message
        self.data_file_name = file_name
        self.m = utils.get_document_by_id('mappings', self.mapping_id)
        lg.info("New data import correcte initialized")
        
    def validate(self):
        lg.info("New import validation process")
        ds_validate = self._validate_datasource()
        db_validate = self._validate_database()
        return (ds_validate and db_validate), self.validation_messages

    def _validate_datasource(self):
        lg.info("Validation process: Data source")
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
            lg.debug("Accessing temporal folder to read the import data file.")
            self.data = pd.read_csv(os.path.join('temp', self.data_file_name), delimiter=self.m['file']['delimiter'], header=self.m['file']['header'])

            data_points = self.m['data_point']

            for k, v in data_points.items():
                self._process_data_point(k, v)

            pp = pprint.PrettyPrinter(indent=4)
            pp.pprint(self.ImportFile)
            pp.pprint(self.ListOfIds)

            return True

        else:
            return False

    def _check_upsert(self, collection, id_list):
        new_items = 0
        update_items = 0
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        for i, (id, value) in enumerate(id_list.items()):
            print(i)
            api_url = '{0}{1}/{2}'.format(config.MONGODB_API_URL,collection, id)
            print((api_url))
            response = requests.get(api_url, headers=headers)
            print(response.content)
            jsonResponse = json.loads(response.content)
            if (response.status_code != 200):
                new_items += 1
            else:
                self.ListOfIds[collection][id]['object_id'] = jsonResponse['_id']
                self.ListOfIds[collection][id]['etag'] = jsonResponse['_etag']
                self.ListOfIds[collection][id]['new'] = False
                update_items +=1
        return new_items, update_items
        
    def _validate_database(self):
        lg.info("Validation process: Database")
        self.validation_messages['DB'] = []
        for key in ['Teams', 'Participants', 'Games', 'GameSections']:
            lg.debug("Validation import objects for collection {0}".format(key))
            new_items, update_items = self._check_upsert(key, self.ListOfIds[key])
            self.validation_messages['DB'].append(create_new_message('text', '', '', '{} CREATE operations in collection {}.'.format(new_items, key)))
            self.validation_messages['DB'].append(create_new_message('text', '', '', '{} UPDATE operations in collection {}.'.format(update_items, key)))

        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(self.ListOfIds)
        return True

    def run(self):
        lg.info("Starting new import.")
        list_of_response = ""
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}
        for key in ['Teams', 'Participants', 'Games', 'GameSections']:
            bulk_inserts = []
            lg.debug("Importing {0}.".format(key))

            for i, value in enumerate(self.ImportFile[key]):
                id, etag, new = self.ListOfIds[key][i]

                if not new:
                    lg.debug("Updating an already existing document in database.")
                    lg.debug("Collection: {0}, Schema identifier: {1}".format(key, value['schema_identifier']))
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

    def _process_data_point(self, dp_name, dp_obj):
        lg.debug("Processing data point: {0}".format(dp_name))

        if (dp_obj['a'] == 'feature'):
            feature_value = dp_obj['value']

            if (check_user_input(feature_value)):
                self.validation_messages['DS'].append(create_new_message('inputText', dp_name, dp_name, dp_obj['description']))
            else:
                lg.debug("Adding feature value to the import file = {0}".format(dp_obj['value']))
                self.ImportFile[dp_name] = (dp_obj['value'])

        elif dp_obj['a'] == 'collection':
            object_id_col = dp_obj['schema_identifier']
            if (check_user_input(object_id_col)):
                self.validation_messages['DS'].append(
                    create_new_message('inputText', dp_name + '#schema_identifier', dp_name + '#schema_identifier',
                                       dp_name + ' schema identifier'))
            else:
                lg.debug("Adding new key to import files.")
                self.ImportFile[dp_name] = {}
                self.ListOfIds[dp_name] = {}

                for i, row in self.data.iterrows():
                    lg.debug("Processing data set row {0}".format(i))
                    object_id = str(row[object_id_col]) \
                        if not object_id_col.startswith('dshs#mapping_') else object_id_col[len('dshs#mapping_'):]

                    if object_id not in self.ListOfIds[dp_name].keys():
                        lg.debug("New instance of {0} with identifier {1}".format(dp_name, object_id))
                        self.ListOfIds[dp_name][object_id] = {}
                        self.ListOfIds[dp_name][object_id]['new'] = False
                        self.ImportFile[dp_name][object_id]= self._newObject(collection=dp_name, value=dp_obj, row=row, id=object_id)

                    else:
                        lg.debug("Updating an existing object of {0} with identifier {1}".format(dp_name, object_id))
                        new_object = self._newObject(collection=dp_name, value=dp_obj, row=row, id=object_id)
                        self._merge_objects(current=self.ImportFile[dp_name][object_id], new=new_object)

    def _newObject(self, collection, value, row, id):
        obj = {}
        obj['schema_identifier'] = str(id)
        lg.debug("Preparing new object for {0} with schema identifier {1}".format(collection, id))

        for nested_key, nested_value in value.items():
            lg.debug("--Processing {}".format(nested_key))

            if type(nested_value) is dict:
                if nested_value['a'] == 'nested_array':
                    if obj.get('{}_index'.format(nested_key)) is None:
                        obj['{}_index'.format(nested_key)] = \
                            [k for k in nested_value.keys() if k not in ["a", "schema_identifier", "additional_fields"]] \
                            + [utils.urlify(add_k) for add_k in nested_value["additional_fields"]]
                    new_nested_array = []
                    for k, v in nested_value.items():
                        if k not in ["a", "schema_identifier", "additional_fields"]:
                            new_nested_array.append(row.get(v))
                        elif k == "additional_fields":
                            for field in v:
                                new_nested_array.append(row.get(field))
                    obj[str(row.get(nested_value['schema_identifier']))] = new_nested_array

                elif nested_value['a'] == 'nested_collection':
                    object_id_col = nested_value['schema_identifier']
                    if (check_user_input(object_id_col)):
                        input_asked = True
                        self.validation_messages['DS'].append(
                            create_new_message('inputText', nested_value + '#schema_identifier',
                                               nested_value + '#schema_identifier',
                                               nested_value + ' schema identifier'))
                    else:
                        object_id = str(row[object_id_col]) if not object_id_col.startswith(
                            'dshs#mapping_') else object_id_col[len('dshs#mapping_'):]
                        obj[nested_key] = {}
                        obj[nested_key][object_id] = self._newObject("{}>{}".format(collection, nested_key), nested_value, row, object_id)

            elif (nested_key not in ['a', 'schema_identifier', 'additional_fields']):
                if check_user_input(nested_value):
                    if len(self.ListOfIds[collection]) == 0:
                        self.validation_messages['DS'].append(create_new_message('inputText', nested_key, nested_key,
                                                                                 'Specify ' + nested_key + ' for object ' + str(
                                                                                     id) + ' in ' + collection + '.'))
                else:
                    obj[nested_key] = row.get(nested_value) if not nested_value.startswith(
                        'dshs#mapping_') else nested_value[len('dshs#mapping_'):]
                    if obj[nested_key] is None:
                        self.validation_messages['DS'].append(create_new_message('alert-danger', '', '',
                                                                                 'Data does not include <<' + nested_value + '>> to get ' + nested_key + ' of object ' + str(
                                                                                     id) + ' in ' + collection))

            elif nested_key == 'additional_fields':
                for additional_column in nested_value:
                    lg.debug("Additional field for {}".format(additional_column))
                    obj[utils.urlify(additional_column)] = row.get(additional_column)
                    if obj[utils.urlify(additional_column)] is None:
                        self.validation_messages['DS'].append(create_new_message('alert-warning', '', '',
                                                                                 'Data does not include <<' + additional_column + '>> of object ' + str(
                                                                                     id) + ' in ' + collection))
        return obj

    def _merge_objects(self, current, new):
        print(current)
        print(new)
        for key, value in new.items():

            if current.get(key) is None:
                current[key] = value
            elif type(value) is dict:
                self._merge_objects(current.get(key), value)
                
        return
