import os
import re
import shutil
import json
import pickle
import logging as lg

from werkzeug.utils import secure_filename

from data_ingestion import config
from data_ingestion.tests import mappings


def check_folder(dir_path, create=True):
    if not os.path.exists(dir_path):
        if create:
            os.makedirs(dir_path)
            return True
        else:
            return False
    else:
        return True

def _url(path):
    return config.MONGODB_API_URL + path


def get_mappings_collection():
    mappings_list = [('0', 'firstMapping'), ('1', 'secondMapping')]

    return mappings_list

def get_document_by_id(collection_name, mapping_number):
    print(mapping_number)
    if mapping_number == '0':
        return mappings.kinexon_mapping
    else:
        return mappings.dfl_mapping

def save_temp_file(f):
    check_folder('temp')
    filename = secure_filename(f.filename)
    f.save(os.path.join('temp', filename))

def dump_pickle_object(object_dict, file_name):
    check_folder('temp')
    lg.debug("Storing {} file as a pickle object serialization".format(file_name))
    output = open(os.path.join('temp', file_name + '.pkl'), 'wb')
    pickle.dump(object_dict, output)
    output.close()

def load_pickle_object(file_name):
    lg.debug("Loading {}".format(file_name))
    input = open(os.path.join('temp', file_name + '.pkl'), 'rb')
    obj_dict = pickle.load(input)
    input.close()
    return obj_dict

def load_temp_file(f):
    check_folder('temp')
    return open(os.path.join('temp', f), "r")

def delete_temp_folder():
    if check_folder('temp', False):
        shutil.rmtree('temp')

def urlify(s):
    # Remove all non-word characters (everything except numbers and letters)
    s = re.sub(r"[^\w\s]", '', s)
    # Replace all runs of whitespace with a single dash
    s = re.sub(r"\s+", '_', s)
    return s

def json_groupby_attribute(docs, attribute):
    grouped_doc = {}
    for doc in docs:
        json_doc = json.loads(doc)
        grouped_doc.setdefault(json_doc[attribute], []).append(json_doc)

    return json.dumps(grouped_doc)

def add_array_index(row, index_list, array_name):
    json_row = json.loads(row)
    json_row["{}_cols".format(array_name)] = index_list
    return json.dumps(json_row)

def get_headers():
    return {'Content-Type': 'application/json', 'Accept': 'application/json'}

def get_default_mapping():
    return mappings.default_mapping

def process_schema(schema_json, level=0):
    json_fields = []
    print(schema_json)
    if schema_json['type'] == 'struct':
        for field in schema_json['fields']:
            new_field = {}
            new_field['level'] = level
            new_field['name'] = field['name']
            if type(field['type']) is not dict:
                new_field['type'] = field['type']
                json_fields.append(new_field)
            else:
                new_field['type'] = field['type']['type']
                json_fields.append(new_field)
                json_fields = json_fields + process_schema(field['type'].get('elementType', field['type']), level + 1)
    else:
        new_field = {}
        new_field['level'] = level
        new_field['name'] = '_element'
        new_field['type'] = "{}({})".format(schema_json['type'], schema_json['elementType'])
        json_fields.append(new_field)

    return json_fields



