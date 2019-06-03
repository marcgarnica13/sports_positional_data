import os
import re
import shutil
import json
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
'''
def get_mappings_collection():
    mappings_list = []
    r = requests.get(_url('/mappings'))
    if r.status_code == requests.codes.ok:
        r_dict = r.json()
        for item in r_dict['_items']:
            new_mapping = {}
            new_mapping['id'] = item['_id']
            new_mapping['name'] = item['name']
            mappings_list.append(new_mapping)
    return mappings_list
'''

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

