import logging as lg
import pandas as pd
import os
import requests
import pprint
import json
import time

from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import functions

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

def add_array_index(row, index_list, array_name):
    json_row = json.loads(row)
    json_row["{}_cols".format(array_name)] = index_list
    return json.dumps(json_row)

def add_nested_collection(row, nested_collection_set, nested_collection_name):
    json_row = json.loads(row)
    json_row["{}".format(nested_collection_name)] = nested_collection_set[json_row.get("schema_identifier")]
    return json.dumps(json_row)

class PreDataImport():

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

    def run(self):
        lg.info("Starting new import.")
        list_of_response = ""
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}

        for key in ['Teams', 'Participants', 'Games', 'GameSections']:
            bulk_inserts = []
            lg.debug("Importing {0}.".format(key))

            for id, document in self.DataFromSource[key].items():
                meta_info = self.ListOfIds[key][id]

                if not meta_info['new']:
                    lg.debug("Updating an already existing document in database.")
                    lg.debug("Collection: {0}, Schema identifier: {1}".format(key, id))
                    etag_headers = headers
                    etag_headers['If-Match'] = meta_info['etag']
                    api_url = '{0}{1}/{2}'.format(config.MONGODB_API_URL, key, meta_info['object_id'])
                    self.response = requests.patch(api_url, headers=etag_headers, data=json.dumps(document))
                    lg.debug(self.response.content)
                    list_of_response += str(self.response.status_code)
                else:
                    bulk_inserts.append(document)


            if len(bulk_inserts) != 0:
                lg.debug('Creating new document in {} collection'.format(key))
                api_url = '{0}{1}'.format(config.MONGODB_API_URL, key)
                self.response = requests.post(api_url, headers=headers, data=json.dumps(bulk_inserts))
                lg.debug(self.response.content)
                list_of_response += str(self.response.status_code)

        return list_of_response

    def _validate_datasource(self):
        lg.info("Validation process: Data source")
        self.DataFromSource = {}
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

            # Features are just one value for the whole import.
            for k, v in data_points.items():
                if v['a'] == 'feature':
                    self._process_feature(k, v)

            # Processing each point of the import data file
            start = time.time()
            for i, row in self.data.iterrows():
                lg.debug("Processing data set row {0}".format(i))
                for k, v in data_points.items():
                    if v['a'] == 'collection':
                        self._process_collection(k, v, row)

            lg.debug("Data file processed in {} seconds.".format(time.time() - start))
            pp = pprint.PrettyPrinter(indent=4)
            pp.pprint(self.DataFromSource)
            pp.pprint(self.ListOfIds)

            return True

        else:
            return False

    def _validate_database(self):
        lg.info("Validation process: Database")
        self.validation_messages['DB'] = []

        for key in ['Teams', 'Participants', 'Games', 'GameSections']:
            lg.debug("Validation database import for collection {0}".format(key))
            new_items, update_items = self._check_upsert(key, self.ListOfIds[key])
            self.validation_messages['DB'].append(create_new_message('text', '', '', '{} CREATE operations in collection {}.'.format(new_items, key)))
            self.validation_messages['DB'].append(create_new_message('text', '', '', '{} UPDATE operations in collection {}.'.format(update_items, key)))

        pp = pprint.PrettyPrinter(indent=4)
        pp.pprint(self.ListOfIds)
        return True

    def _check_upsert(self, collection, id_list):
        new_items = 0
        update_items = 0
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}

        for i, (id, value) in enumerate(id_list.items()):
            api_url = '{0}{1}/{2}'.format(config.MONGODB_API_URL,collection, id)
            response = requests.get(api_url, headers=headers)
            print(response.content)
            jsonResponse = json.loads(response.content)
            if (response.status_code != 200):
                new_items += 1
                self.ListOfIds[collection][id]['new'] = True
            else:
                self.ListOfIds[collection][id]['object_id'] = jsonResponse['_id']
                self.ListOfIds[collection][id]['etag'] = jsonResponse['_etag']
                self.ListOfIds[collection][id]['new'] = False
                update_items +=1
        return new_items, update_items

    def _process_feature(self, feature_name, feature_value):

        if (check_user_input(feature_value)):
            self.validation_messages['DS'].append(create_new_message('inputText', feature_name, feature_name, feature_value['description']))
        else:
            lg.debug("Adding feature value to the import file = {0}".format(feature_value['value']))
            self.DataFromSource[feature_name] = (feature_value['value'])

    def _process_collection(self, dp_name, dp_obj, row):
        lg.debug("Processing data point: {0}".format(dp_name))

        object_id_col = dp_obj['schema_identifier']

        if (check_user_input(object_id_col)):
            self.validation_messages['DS'].append(
                create_new_message('inputText', dp_name + '#schema_identifier', dp_name + '#schema_identifier',
                                   dp_name + ' schema identifier'))

        else:
            object_id = str(row[object_id_col]) \
                if not object_id_col.startswith('dshs#mapping_') else object_id_col[len('dshs#mapping_'):]

            if object_id not in self.ListOfIds.setdefault(dp_name, {}).keys():
                lg.debug("New instance of {0} with identifier {1}".format(dp_name, object_id))
                self.ListOfIds.setdefault(dp_name, {})[object_id] = {}
                self.DataFromSource.setdefault(dp_name, {})[object_id] = self._new_object(collection=dp_name, value=dp_obj, row=row, id=object_id)

            else:
                lg.debug("Updating an existing object of {0} with identifier {1}".format(dp_name, object_id))
                new_object = self._new_object(collection=dp_name, value=dp_obj, row=row, id=object_id)
                self._merge_objects(current=self.DataFromSource[dp_name][object_id], new=new_object)

    def _new_object(self, collection, value, row, id):
        obj = {}
        obj['schema_identifier'] = str(id)
        lg.debug("Preparing new object for {0} with schema identifier {1}".format(collection, id))

        for nested_key, nested_value in value.items():
            lg.debug(" >> Processing {}".format(nested_key))

            if type(nested_value) is dict:
                if nested_value['a'] == 'nested_array':
                    new_nested_array = []

                    for k, v in nested_value.items():
                        if k not in ["a", "schema_identifier", "additional_fields"]:

                            new_nested_array.append(row.get(v))
                        elif k == "additional_fields":
                            for field in v:
                                new_nested_array.append(row.get(field))

                    obj[str(row.get(nested_value['schema_identifier']))] = new_nested_array
                    obj['{}_rows'.format(nested_key)] = []
                    obj['{}_rows'.format(nested_key)].append(row.get(nested_value['schema_identifier']))
                    obj['{}_cols'.format(nested_key)] = \
                        [k for k in nested_value.keys() if k not in ["a", "schema_identifier", "additional_fields"]] \
                        + [utils.urlify(add_k) for add_k in nested_value["additional_fields"]]

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
                        obj[nested_key][object_id] = self._new_object("{}>{}".format(collection, nested_key), nested_value, row, object_id)

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


class DataImport():

    def __init__(self, mapping, name, message, file_name):
        lg.info("New data import: {0}-{1}-{2}-{3}".format(mapping, name, message, file_name))
        self.mapping_id = mapping
        self.author = name
        self.import_message = message
        self.data_file_name = file_name
        self.m = utils.get_document_by_id('mappings', self.mapping_id)

        self.sc = SparkContext.getOrCreate()
        self.sqlContext = SQLContext(sparkContext=self.sc)

        lg.info("New data import correcte initialized")

    def validate(self):
        lg.info("New import validation process")
        ds_validate = self._validate_datasource()
        db_validate = self._validate_database()
        return (ds_validate and db_validate), self.validation_messages

    def run(self):
        lg.info("Starting new import.")
        list_of_response = ""
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}

        for collection, json_doc_set in self.IndividualImport.items():
            for doc in json_doc_set:
                lg.debug("Updating an already existing document in database.")
                lg.debug("Collection: {0}, Schema identifier: {1}".format(collection, doc['schema_identifier']))
                etag_headers = headers
                etag_headers['If-Match'] = self.MetaImport[collection][doc['schema_identifier']]['etag']
                api_url = '{0}{1}/{2}'.format(config.MONGODB_API_URL, collection, self.MetaImport[collection][doc['schema_identifier']]['object_id'])
                self.response = requests.patch(api_url, headers=etag_headers, data=json.dumps(doc))
                print(self.response.content)
                list_of_response += str(self.response.status_code)

        for collection, json_doc_set in self.BulkImport.items():
            lg.debug('Creating new document in {} collection'.format(collection))
            api_url = '{0}{1}'.format(config.MONGODB_API_URL, collection)
            self.response = requests.post(api_url, headers=headers, data=json.dumps(json_doc_set))
            lg.debug(self.response.content)
            list_of_response += str(self.response.status_code)

        return list_of_response

    def _validate_datasource(self):
        lg.info("Validation process: Data source")
        self.DataFromSource = {}
        self.BulkImport = {}
        self.IndividualImport = {}
        self.MetaImport = {}


        self.validation_messages = {}
        self.validation_messages['DS'] = []

        self.validation_messages['DS'].append(create_new_message('text', '', '', 'Import author: ' + self.author))
        self.validation_messages['DS'].append(
            create_new_message('text', '', '', 'Import message: ' + self.import_message))
        self.validation_messages['DS'].append(
            create_new_message('text', '', '', 'Import data file: ' + self.data_file_name))
        self.validation_messages['DS'].append(
            create_new_message('text', '', '', 'Data file mapping: ' + self.m['name']))
        self.validation_messages['DS'].append(
            create_new_message('text', '', '', 'Data file format: ' + self.m['file']['format']))

        if utils.check_folder(os.path.join('temp', self.data_file_name), False):
            lg.debug("Accessing temporal folder to read the import data file.")
            self.df = self.sqlContext.read.csv(os.path.join('temp', self.data_file_name), header=self.m['file']['header'], sep=self.m['file']['delimiter'], inferSchema=True)

            for data_key, data_definition in self.m['data'].items():
                if data_definition['a'] == 'feature':
                    self._process_feature(data_key, data_definition)
                elif data_definition['a'] == 'collection':
                    self._process_collection(data_key, data_definition)

            print(self.DataFromSource)
            return True

        else:
            return False

    def _validate_database(self, dry_run=True):
        lg.info("Validation process: Database")
        self.validation_messages['DB'] = []

        for collection, doc_set in self.DataFromSource.items():
            lg.debug("Validation database import for collection {0}".format(collection))
            new_items, update_items = self._check_upsert(collection, doc_set)
            self.validation_messages['DB'].append(
                create_new_message('text', '', '', '{} CREATE operations in collection {}.'.format(new_items, collection)))
            self.validation_messages['DB'].append(
                create_new_message('text', '', '', '{} UPDATE operations in collection {}.'.format(update_items, collection)))

        return True

    def _check_upsert(self, collection, doc_set):
        new_items = 0
        update_items = 0
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}

        for doc in doc_set:
            json_doc = json.loads(doc)
            api_url = '{0}{1}/{2}'.format(config.MONGODB_API_URL, collection, json_doc['schema_identifier'])
            response = requests.get(api_url, headers=headers)
            print(response.content)
            jsonResponse = json.loads(response.content)
            if (response.status_code != 200):
                new_items += 1
                self.BulkImport.setdefault(collection, []).append(json_doc)
            else:
                self.IndividualImport.setdefault(collection, []).append(json_doc)
                self.MetaImport.setdefault(collection, {}).setdefault(json_doc['schema_identifier'], {})['object_id'] = jsonResponse['_id']
                self.MetaImport[collection][json_doc['schema_identifier']]['etag'] = jsonResponse['_etag']
                update_items += 1

        return new_items, update_items

    def _process_feature(self, feature_name, feature_value):

        if (check_user_input(feature_value['value'])):
            self.validation_messages['DS'].append(
                create_new_message('inputText', feature_name, feature_name, feature_value['description']))
        else:
            lg.debug("Adding feature value to the import file = {0}".format(feature_value['value']))
            self.DataFromSource[feature_name] = (feature_value['value'])

    def _process_collection(self, collection_name, collection_description, dry_run=True, nested=False):
        lg.debug("Processing data point: {0}".format(collection_name))

        object_id_col = collection_description['schema_identifier']

        if (check_user_input(object_id_col)):
            self.validation_messages['DS'].append(
                create_new_message('inputText', collection_name + '#schema_identifier', collection_name + '#schema_identifier',
                                   collection_name + ' schema identifier'))

        else:
            select_cols = []
            select_alias_mapping = []
            array_cols = []
            array_alias_mapping = []
            select_literals = []
            select_literals_alias = []

            for attribute, pointer in collection_description.items():
                lg.debug(" >> Processing {}".format(attribute))
                if type(pointer) is not dict and attribute not in ['a', 'additional_fields']:
                    is_mapping, mapping_word = check_mapping_input(pointer)
                    if (is_mapping):
                        lg.debug("Processing a mapping {}".format(pointer))
                        select_literals.append(mapping_word)
                        select_literals_alias.append(attribute)
                    elif check_user_input(pointer) and (self.ListOfIds.setdefault(collection_name, {}).get(object_id_col) is None):
                        lg.debug("Processing an input {}".format(pointer))
                        self.validation_messages['DS'].append(
                            create_new_message('inputText', attribute, attribute, "Specify {0} column for collection {1}".format(attribute, collection_name))
                        )
                    else:
                        select_cols.append(pointer)
                        select_alias_mapping.append(attribute)

                elif attribute == "additional_fields":
                    for field in pointer:
                        select_cols.append(field)
                        select_alias_mapping.append(utils.urlify(field))

                elif type(pointer) is dict:
                    if pointer['a'] == "nested_collection":
                        nested_collection_name = attribute
                        lg.debug("Processing a nested collection dict {}".format(attribute))
                        join_key = pointer.pop('join_key', None)
                        self._process_collection(attribute, pointer)
                    elif pointer['a'] == "nested_array":
                        nested_array_name = attribute
                        lg.debug("Processing a nested array dict {}".format(attribute))
                        for k, v in pointer.items():
                            if k not in ["a", "additional_fields"]:
                                array_cols.append(v)
                                array_alias_mapping.append(k)
                            elif k == "additional_fields":
                                for field in v:
                                    array_cols.append(field)
                                    array_alias_mapping.append(utils.urlify(field))

            if (len(select_cols) + len(select_literals) != 0):
                if len(array_cols) == 0:
                    lg.debug(
                        "Running select operation on pyspark dataframe with select attributes {0}, select literals {1}".format(
                            select_cols, select_literals))

                    collection_data = self.df.select([functions.col(c).alias(select_alias_mapping[i]) for i,c in enumerate(select_cols)] + [functions.lit(m).alias(select_literals_alias[i]) for i,m in enumerate(select_literals)])\
                    .distinct().toJSON()

                    self.DataFromSource[collection_name] = collection_data.collect()


                else:
                    lg.debug(
                        "Running select operation on pyspark dataframe with select attributes {0}, select literals {1}, group by {0}, aggregating fields {2}".format(
                            select_cols, select_literals, array_cols))

                    collection_data = self.df.select(
                        [functions.col(c).alias(select_alias_mapping[i]) for i,c in enumerate(select_cols)] +
                        [functions.lit(c).alias(select_literals_alias[i]) for i,c in enumerate(select_literals)] +
                        [functions.col(c).alias(array_alias_mapping[i]) for i,c in enumerate(array_cols)]
                    )\
                    .groupBy(
                        [functions.col(c) for c in select_alias_mapping] +
                        [functions.col(m) for m in select_literals_alias]
                    )\
                    .agg(
                        functions.collect_list(functions.struct(*[c for c in (array_alias_mapping)])).alias(nested_array_name)
                    ).toJSON().map(lambda row: add_array_index(row, index_list=array_alias_mapping, array_name=nested_array_name))

                    self.DataFromSource[collection_name] = collection_data.collect()

