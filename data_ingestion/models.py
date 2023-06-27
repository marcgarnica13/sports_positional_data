import logging as lg
import os
import json
import time

from jsonmerge import merge as jsmerge
from pathlib import Path

import config, utils, mongo_api
from file_processor import basic, csv_processor, json_processor, xml_processor

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
            delimiter=file_description['delimiter'],
            time_format=file_description['timestamp_format']
        )
    elif file_description['format'] == 'xml':
        return xml_processor.XMLProcessor(
            file_path=os.path.join('temp', file_name),
            rowtag=file_description['rowTag'],
            value_tag=file_description['valueTag'],
            time_format=file_description['timestamp_format']
        )
    elif file_description['format'] == 'json':
        return json_processor.JSONProcessor(
            file_path=os.path.join('temp', file_name),
            time_format=file_description['timestamp_format']
        )

class DataImport():

    def __init__(self, mapping, name, message, file_name):
        self.mapping_id = mapping
        self.author = name
        self.import_message = message
        self.data_file_name = file_name
        lg.info("New data import: {0}-{1}-{2}-{3}".format(mapping, name, message, file_name))

    def get_application_id(self):
        return self.fp.spark.sparkContext.applicationId

    def validate(self):
        lg.info("New import validation process")
        code, response_code, response_content = mongo_api.get_document(collection_name='mappings', document_key=self.mapping_id, database_attributes=False)
        if code != 0:
            return False, "Error loading mapping", ""
        else:
            self.m = response_content

        ds_validate = self._validate_datasource()
        db_validate = self._validate_database()
        self._store_validation_results()
        return (ds_validate and db_validate), self.validation_messages, self.fp.metadata

    def load_results(self):
        self._load_import_files()
        return True, self.validation_messages, self.file_metadata

    def run(self):
        self._load_import_files()
        utils.delete_temp_folder()
        lg.info("Starting new import.")
        list_of_responses = {}

        for collection, json_doc_set in self.IndividualImport.items():
            for doc in json_doc_set:
                lg.debug("Updating an already existing document in database.")
                lg.debug("Collection: {0}, Schema identifier: {1}".format(collection, doc['schema_identifier']))
                code, response_code, response_content = mongo_api.patch_document(collection_name=collection,
                                         json_doc=json.dumps(doc),
                                         etag=self.MetaImport[collection][doc['schema_identifier']]['etag'],
                                         database_id=self.MetaImport[collection][doc['schema_identifier']]['object_id'])
                if code == 0:
                    list_of_responses.setdefault(collection, []).append(
                        create_new_message('text', doc['schema_identifier'], '', 'Document {} : Update executed correctly'.format(doc['schema_identifier'])))
                else:
                    list_of_responses.setdefault(collection, []).append(
                        create_new_message('text', doc['schema_identifier'], '', 'Error while updating document {}: {} -> {}'.format(doc['schema_identifier'], response_code, response_content))
                    )


        for collection, json_doc_set in self.BulkImport.items():
            lg.debug('Creating new document in {} collection'.format(collection))
            code, response_code, response_content = mongo_api.post_document(collection_name=collection, json_doc=json.dumps(json_doc_set))
            if code == 0:
                list_of_responses.setdefault(collection, []).append(
                    create_new_message('text', '', '', 'Creating {} new documents successfully'.format(len(json_doc_set))))
            else:
                list_of_responses.setdefault(collection, []).append(
                    create_new_message('text', '', '',
                                       'Error while creating {} new documents: {} -> {}'.format(len(json_doc_set), response_code, response_content))
                )

        return list_of_responses

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

        start = time.time()
        if utils.check_folder(os.path.join('temp', self.data_file_name), False):
            lg.debug("Accessing temporal folder to read the import data file.")
            #self.fileProcessor = utils.new_file_processor(self.m['file']['format'])
            self.fp = new_file_processor(self.m['file'], self.data_file_name)


            for data_key, data_value in self.m['data'].items():
                for data_definition in data_value:
                    if data_definition['a'] == 'feature':
                        self._process_feature(data_key, data_definition)
                    elif data_definition['a'] == 'collection':
                        start = time.time()
                        self.DataFromSource.setdefault(data_key, []).extend(self._process_collection(data_key, data_definition))
                        lg.debug("Data source processed for collection {} in {} seconds".format(data_key, time.time() - start))

            self.validation_messages['DS'].append(
                create_new_message('text', '', '', 'Data source validated in {} seconds'.format(time.time() - start)))


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
            code, response_code, json_content = mongo_api.get_document(collection_name=collection, document_key=json_doc['schema_identifier'])
            if code != 0:
                new_items += 1
                self.BulkImport.setdefault(collection, []).append(json_doc)
            else:
                self.IndividualImport.setdefault(collection, []).append(
                    jsmerge({key:val for key, val in json_content.items() if key not in mongo_api.get_mongo_api_attributes()} , json_doc))
                self.MetaImport.setdefault(collection, {}).setdefault(json_doc['schema_identifier'], {})['object_id'] = json_content['_id']
                self.MetaImport[collection][json_doc['schema_identifier']]['etag'] = json_content['_etag']
                update_items += 1

        return new_items, update_items

    def _process_feature(self, feature_name, feature_value):

        if (check_user_input(feature_value['value'])):
            self.validation_messages['DS'].append(
                create_new_message('inputText', feature_name, feature_name, feature_value['description']))
        else:
            lg.debug("Adding feature value to the import file = {0}".format(feature_value['value']))
            self.DataFromSource[feature_name] = (feature_value['value'])

    def _process_collection(self, collection_name, collection_description, nested=False):
        if not nested:
            self.fp.reset_cols()

        lg.debug("Processing data point: {0}".format(collection_name))

        object_id_col = ''
        if nested:
            object_id_col = collection_description['n_id']
        else:
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
                        self.fp.append_select_literals(literal=mapping_word, literal_alias=attribute, nested=nested)
                    elif check_user_input(pointer) and (self.ListOfIds.setdefault(collection_name, {}).get(object_id_col) is None):
                        lg.debug("Processing an input {}".format(pointer))
                        self.validation_messages['DS'].append(
                            create_new_message('inputText', attribute, attribute, "Specify {0} column for collection {1}".format(attribute, collection_name))
                        )
                    else:
                        self.fp.append_select_columns(column=pointer, column_alias=attribute, nested=nested)

                elif attribute == "additional_fields":
                    for field in pointer:
                        self.fp.append_select_columns(column=field, column_alias=utils.urlify(field), nested=nested)

                elif attribute == "partition":
                    self.fp.set_partition(pointer['interval'], pointer['ts_field'])

                elif type(pointer) is dict:
                    if pointer['a'] == "nested_collection":
                        nested_collection_name = attribute
                        self.fp.set_nested_collection_name(nested_collection_name)
                        lg.debug("Processing a nested collection dict {}".format(attribute))
                        self._process_collection(attribute,pointer,True)
                        lg.debug("Data source processed for collection {}".format(attribute))

                    elif pointer['a'] == "nested_array":
                        self.fp.set_nested_array_name(attribute, nested)
                        lg.debug("Processing a nested array dict {}".format(attribute))
                        for k, v in pointer.items():
                            if k not in ["a", "additional_fields"]:
                                self.fp.append_array_columns(array_column=v, array_column_alias=k, nested=nested)
                            elif k == "additional_fields":
                                for field in v:
                                    self.fp.append_array_columns(array_column=field, array_column_alias=utils.urlify(field), nested=nested)

            if not nested:
                job_text = "{}#{}#Exploding data source and extracting data".format(self.data_file_name, collection_name)
                self.fp.set_job_description(job_text)
                return self.fp.process()

    def _store_validation_results(self):
        temp_file_name = Path(self.data_file_name).stem
        utils.dump_pickle_object(self.validation_messages, temp_file_name + "_validation_methods")
        utils.dump_pickle_object(self.fp.metadata, temp_file_name + "_structure")
        utils.dump_pickle_object(self.BulkImport, temp_file_name + "_bulk")
        utils.dump_pickle_object(self.IndividualImport, temp_file_name + "_individual")
        utils.dump_pickle_object(self.MetaImport, temp_file_name + "_meta")

    def _load_import_files(self):
        temp_file_name = Path(self.data_file_name).stem
        self.validation_messages = utils.load_pickle_object(temp_file_name + "_validation_methods")
        self.file_metadata = utils.load_pickle_object(temp_file_name + "_structure")
        self.BulkImport = utils.load_pickle_object(temp_file_name + "_bulk")
        self.IndividualImport = utils.load_pickle_object(temp_file_name + "_individual")
        self.MetaImport = utils.load_pickle_object(temp_file_name + "_meta")
