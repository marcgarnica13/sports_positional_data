import logging as lg
import os
import requests
import json
import time

from data_ingestion import config
from data_ingestion import utils
from data_ingestion.file_processor import basic, csv_processor, json_processor, xml_processor

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

def new_file_processor(file_description, file_name):
    if file_description['format'] == 'csv':
        return csv_processor.CSVProcessor(
            file_path=os.path.join('temp', file_name),
            header=file_description['header'],
            delimiter=file_description['delimiter'])
    elif file_description['format'] == 'xml':
        return xml_processor.XMLProcessor(
            file_path=os.path.join('temp', file_name),
            rowtag=file_description['rowTag']
        )

class DataImport():

    def __init__(self, mapping, name, message, file_name):
        lg.info("New data import: {0}-{1}-{2}-{3}".format(mapping, name, message, file_name))
        self.mapping_id = mapping
        self.author = name
        self.import_message = message
        self.data_file_name = file_name
        self.m = utils.get_document_by_id('mappings', self.mapping_id)
        lg.info("New data import initialized")

    def validate(self):
        lg.info("New import validation process")
        ds_validate = self._validate_datasource()
        db_validate = True
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
        self.NestedCollections = {}
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

        start = time.time()
        if utils.check_folder(os.path.join('temp', self.data_file_name), False):
            lg.debug("Accessing temporal folder to read the import data file.")
            #self.fileProcessor = utils.new_file_processor(self.m['file']['format'])
            self.fp = new_file_processor(self.m['file'], self.data_file_name)

            for data_key, data_definition in self.m['data'].items():
                if data_definition['a'] == 'feature':
                    self._process_feature(data_key, data_definition)
                elif data_definition['a'] == 'collection':
                    self.DataFromSource[data_key] = self._process_collection(data_key, data_definition)
                    lg.debug("Data source processed for collection {}".format(data_key))
                print(self.DataFromSource)

            self.validation_messages['DS'].append(
                create_new_message('text', '', '', 'Data source validated in {} seconds'.format(time.time() - start)))

            print(self.DataFromSource)
            print(self.NestedCollections)
            return True
        else:
            return False

    def _validate_database(self, dry_run=True):
        lg.info("Validation process: Database")
        self.validation_messages['DB'] = []
        start = time.time()

        for collection, doc_set in self.DataFromSource.items():
            lg.debug("Validation database import for collection {0}".format(collection))
            new_items, update_items = self._check_upsert(collection, doc_set)
            self.validation_messages['DB'].append(
                create_new_message('text', '', '', '{} CREATE operations in collection {}.'.format(new_items, collection)))
            self.validation_messages['DB'].append(
                create_new_message('text', '', '', '{} UPDATE operations in collection {}.'.format(update_items, collection)))

        self.validation_messages['DB'].append(
            create_new_message('text', '', '', 'Database validated for the import in {} seconds'.format(time.time() - start)))
        return True

    def _check_upsert(self, collection, doc_set):
        new_items = 0
        update_items = 0
        headers = {'Content-Type': 'application/json', 'Accept': 'application/json'}

        for doc in doc_set:
            json_doc = json.loads(doc)
            json_doc['schema_identifier'] = str(json_doc['schema_identifier'])
            api_url = '{0}{1}/{2}'.format(config.MONGODB_API_URL, collection, json_doc['schema_identifier'])
            response = requests.get(api_url, headers=headers)
            jsonResponse = json.loads(response.content)
            nested_length = self._add_nested_collection(collection, json_doc)
            if nested_length > 0:
                self.validation_messages['DS'].append(
                    create_new_message('text', '', '','{0} nested objects for {1} with id {2}'.format(nested_length, collection, json_doc['schema_identifier'])))
            lg.debug(json_doc)

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
        self.fp.reset_cols()
        lg.debug("Processing data point: {0}".format(collection_name))

        object_id_col = collection_description['schema_identifier']

        if (check_user_input(object_id_col)):
            self.validation_messages['DS'].append(
                create_new_message('inputText', collection_name + '#schema_identifier', collection_name + '#schema_identifier',
                                   collection_name + ' schema identifier'))

        else:
            for attribute, pointer in collection_description.items():
                lg.debug(" >> Processing {}".format(attribute))
                if type(pointer) is not dict and attribute not in ['a', 'additional_fields']:
                    is_mapping, mapping_word = check_mapping_input(pointer)
                    if (is_mapping):
                        lg.debug("Processing a mapping {}".format(pointer))
                        self.fp.append_select_literals(literal=mapping_word, literal_alias=attribute)
                    elif check_user_input(pointer) and (self.ListOfIds.setdefault(collection_name, {}).get(object_id_col) is None):
                        lg.debug("Processing an input {}".format(pointer))
                        self.validation_messages['DS'].append(
                            create_new_message('inputText', attribute, attribute, "Specify {0} column for collection {1}".format(attribute, collection_name))
                        )
                    else:
                        self.fp.append_select_columns(column=pointer, column_alias=attribute)

                elif attribute == "additional_fields":
                    for field in pointer:
                        self.fp.append_select_columns(column=field, column_alias=utils.urlify(field))

                elif type(pointer) is dict:
                    if pointer['a'] == "nested_collection":
                        nested_collection_name = attribute
                        lg.debug("Processing a nested collection dict {}".format(attribute))
                        join_key = pointer.pop('join_key', None)
                        lg.debug("Grouping document by attribute {}".format(attribute))
                        c,ca,l,la,a,aa, nested_name = self.fp.copy_processor_arrays()
                        self.NestedCollections.setdefault(collection_name, {})[attribute] = utils.json_groupby_attribute(self._process_collection(attribute,pointer), join_key)
                        lg.debug("Data source processed for collection {}".format(attribute))
                        self.fp.set_processor_arrays(c,ca,l,la,a,aa)
                        pointer['join_key'] =  join_key
                    elif pointer['a'] == "nested_array":
                        self.fp.set_nested_array_name(attribute)
                        lg.debug("Processing a nested array dict {}".format(attribute))
                        for k, v in pointer.items():
                            if k not in ["a", "additional_fields"]:
                                self.fp.append_array_columns(array_column=v, array_column_alias=k)
                            elif k == "additional_fields":
                                for field in v:
                                    self.fp.append_array_columns(array_column=field, array_column_alias=utils.urlify(field))

            return self.fp.process()

    def _add_nested_collection(self, collection_name, json_document):
        nested_length = 0

        for nested, nested_values in self.NestedCollections.get(collection_name, {}).items():
            nested_json = json.loads(nested_values)
            nested_length = len(nested_json[json_document['schema_identifier']])
            json_document[nested] = nested_json[json_document['schema_identifier']]

        return nested_length
