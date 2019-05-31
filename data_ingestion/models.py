import logging as lg
import os
import requests
import json
import time

from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext, SparkSession
from pyspark.sql import functions

from data_ingestion import config
from data_ingestion import utils

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

class DataImport():

    def __init__(self, mapping, name, message, file_name):
        lg.info("New data import: {0}-{1}-{2}-{3}".format(mapping, name, message, file_name))
        self.mapping_id = mapping
        self.author = name
        self.import_message = message
        self.data_file_name = file_name
        self.m = utils.get_document_by_id('mappings', self.mapping_id)

        self.sc = SparkContext.getOrCreate()
        lg.getLogger('pyspark').setLevel(lg.ERROR)
        self.sqlContext = SQLContext(sparkContext=self.sc)

        lg.info("New data import initialized")

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

        if utils.check_folder(os.path.join('temp', self.data_file_name), False):
            lg.debug("Accessing temporal folder to read the import data file.")
            #self.fileProcessor = utils.new_file_processor(self.m['file']['format'])
            self.df = self.sqlContext.read.csv(os.path.join('temp', self.data_file_name), header=self.m['file']['header'], sep=self.m['file']['delimiter'], inferSchema=True)

            for data_key, data_definition in self.m['data'].items():
                if data_definition['a'] == 'feature':
                    self._process_feature(data_key, data_definition)
                elif data_definition['a'] == 'collection':
                    self.DataFromSource[data_key] = self._process_collection(data_key, data_definition).collect()

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
            json_doc['schema_identifier'] = str(json_doc['schema_identifier'])
            api_url = '{0}{1}/{2}'.format(config.MONGODB_API_URL, collection, json_doc['schema_identifier'])
            response = requests.get(api_url, headers=headers)
            jsonResponse = json.loads(response.content)
            self._add_nested_collection(collection, json_doc)
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
        lg.debug("Processing data point: {0}".format(collection_name))
        collection_data = []

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
                        print("JOIN_KEY ", join_key)
                        lg.debug("Grouping document by attribute {}".format(attribute))
                        self.NestedCollections.setdefault(collection_name, {})[attribute] = utils.json_groupby_attribute(self._process_collection(attribute,pointer).collect(), join_key)
                        pointer['join_key'] =  join_key
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

            return collection_data

    def _add_nested_collection(self, collection_name, json_document):

        for nested, nested_values in self.NestedCollections.get(collection_name, {}).items():
            nested_json = json.loads(nested_values)
            json_document[nested] = nested_json[json_document['schema_identifier']]

        return

class XMLDataImport():

    def __init__(self, mapping, name, message, file_name):
        lg.info("New data import: {0}-{1}-{2}-{3}".format(mapping, name, message, file_name))
        self.mapping_id = mapping
        self.author = name
        self.import_message = message
        self.data_file_name = file_name
        self.m = utils.get_document_by_id('mappings', self.mapping_id)

        #self.sc = SparkContext.getOrCreate()
        #self.sqlContext = SQLContext(sparkContext=self.sc)

        lg.info("New data import initialized")

    def validate(self):
        lg.info("New import validation process")
        ds_validate = self._validate_datasource()
        return ds_validate, self.validation_messages

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

        if utils.check_folder(os.path.join('temp', self.data_file_name), False):
            lg.debug("Accessing temporal folder to read the import data file.")
            conf = SparkConf()
            conf.set('spark.logConf', 'true')
            spark = SparkSession.builder.config(conf=conf).getOrCreate()
            spark.sparkContext.setLogLevel("ERROR")
            start = time.time()
            self.df = spark.read.format('xml').options(rowtag='FrameSet').load(os.path.join('temp', self.data_file_name))
            self.df.printSchema()

            frame_df = self.df.select('*', functions.explode("Frame").alias("Moment")).select('*', functions.col("Moment._N").alias("Moment_id")).\
                groupBy("Moment_id", "Moment._T", "_GameSection", "Moment._BallStatus", "Moment._BallPossession").agg(functions.collect_list(functions.struct("_PersonId", "Moment._M", "Moment._S", "Moment._T", "Moment._X", "Moment._Y" "Moment._Z")).alias("participants")).persist()

            frame_df.printSchema()
            print(frame_df.filter(functions.col('Moment_id') == 100001).show(20, False))
            #print(frame_df.toJSON().collect())
            #print(self.df.select('Frame').show(10, False))
            print("{} seconds".format(time.time() - start))

        '''
            for data_key, data_definition in self.m['data'].items():
                if data_definition['a'] == 'feature':
                    self._process_feature(data_key, data_definition)
                elif data_definition['a'] == 'collection':
                    self.DataFromSource[data_key] = self._process_collection(data_key, data_definition).collect()

            return True

        else:
            return False
        
        '''
