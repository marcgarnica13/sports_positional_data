import requests
import json
import time
import logging as lg

from deepmerge import always_merger

from data_ingestion import config, utils

def _url(path):
    """
    Helper: MongoDB API URL builder

    Args:
        path: str
            Collection and document specific path. Ex: mappings/1 or Games/9

    Returns:
        _ : str
            Complete and functional API url FOR MongoDB REST API

    """
    return config.MONGODB_API_URL + path

def get_collection(collection_name, all_collection=True):
    """
    Get collection from MongoDB API

    Args:
        collection_name: str
            Name of the collection

    Returns:
        collection_list: list(JSON)
            List of JSON documents queried from the MongoDb collection
    """
    collection_list = []
    r = requests.get(_url("{}".format(collection_name)))
    page=1
    finished = False
    # MongoDB pagination
    while not finished:
        if r.status_code == requests.codes.ok:
            r_dict = r.json()
            for item in r_dict.get('_items', []):
                new_doc = item
                collection_list.append(new_doc)
            if r_dict['_meta']['total'] > r_dict['_meta']['max_results']:
                page = page + 1
                r = requests.get(_url("/{}?page={}".format(collection_name, page)))
            else:
                finished = True
        elif not all_collection:
            finished = True
        else:
            pass
    return collection_list

def get_merged_collection(collection_name, initial_template={}):
    collection_documents = get_collection(collection_name, False)
    print(initial_template)
    for d in collection_documents:
        filtered_dict = {key: val for key, val in d.items() if key not in get_mongo_api_attributes()}
        always_merger.merge(initial_template, filtered_dict)
    return initial_template

def get_mongo_api_attributes():
    """

    Returns:
        _ : list(str)
            List of MongoDB API additional attributes

    """
    """ Helper: providing the MongoDB API additional attributes
    ----------------------------------------------------------
    Parameters
    ----------
    Returns
    -------
    _ : list(str)
        List of MongoDB API additional attributes
    """
    return ['_id', '_created', '_updated', '_links', '_etag']

def import_new_document(collection_name, new_doc, document_key, overwrite=False):
    """
    Import new document into MongoDB

    Args:
        collection_name: str
            Name of the collection
        new_doc: JSON object
            Document to import
        document_key: str
            Domain primary key attribute of the document
        overwrite:  boolean(False)
            If TRUE, overwrite already existing content

    Returns:
        _: int
            Result code 0-success 1-failure
        _: str
            Result message
        _: JSON Object
            Extra result content in a JSON format

    """
    key = new_doc[document_key]
    start = time.time()
    lg.debug("Starting new import for {} collection".format(collection_name))
    if key is None:
        lg.debug("Import failed in {} seconds: The key value does not exist in the document".format(time.time() - start))
        return 1, "{} value does not exists in the new document for collection {}".format(document_key, collection_name)

    code, response_code, response_content = get_document(collection_name, key)
    if code == 0:
        if overwrite:
            code, response_code, response_content = put_document(collection_name, json.dumps(new_doc),
                                                                        response_content['_etag'],
                                                                        response_content['_id'])
            if code != 0:
                return 3, "Put operation on {} with {} = {} failed.".format(collection_name, document_key,
                                                                      key), response_content
        else:
            return 2, \
            "Document with {} = {} already exists in the database collection {}".format(
                document_key, key, collection_name), \
            response_content
    else:
        code, response_code, response_content = post_document(collection_name, json.dumps(new_doc))
        if code != 0:
            return 4, "Post operation on {} failed".format(collection_name), response_content

    lg.debug("Import done in {} seconds".format(time.time() - start))
    return 0, "", response_content

def get_document(collection_name, document_key, database_attributes=True):
    """

    Args:
        collection_name: str
            Name of the collction
        document_key: str
            Domain primary key attribute of the document
        database_attributes: boolean(True)
            Include database specific attributes in the retrieving document

    Returns:
        _: int
            Result code 0-success 1-failure
        _: str
            Result message
        _: JSON Object
            Extra result content in a JSON format

    """
    response = requests.get(_url("{}/{}".format(collection_name, document_key)), headers=utils.get_headers())
    if response.status_code == 200:
        if database_attributes:
            return 0, response.status_code, json.loads(response.content)
        else:
            return 0, response.status_code, {key:val for key,val in json.loads(response.content).items() if key not in get_mongo_api_attributes()}
    else:
        return 1, response.status_code, json.loads(response.content)

def patch_document(collection_name, json_doc, etag, database_id):
    """
    Args:
        collection_name: str
            Name of the collection
        json_doc: JSON object
            Document
        etag: long
            Hash_function tag for concurrency management
        database_id:
            Database id

    Returns:
        _: int
            Result code 0-success 1-failure
        _: str
            Result message
        _: JSON Object
            Extra result content in a JSON format
    """
    headers = utils.get_headers()
    headers['If-Match'] = etag
    response = requests.patch(_url("{}/{}".format(collection_name, database_id)), headers=headers, data=json_doc)

    if response.status_code == 200:
        return 0, response.status_code, json.loads(response.content)
    else:
        return 1, response.status_code, json.loads(response.content)

def post_document(collection_name, json_doc):
    """

    Args:
        collection_name: str
            Name of the collection
        json_doc: JSON object
            Document
        etag: long
            Hash_function tag for concurrency management
        database_id:
            Database id

    Returns:
        _: int
            Result code 0-success 1-failure
        _: str
            Result message
        _: JSON Object
            Extra result content in a JSON format

    """
    response = requests.post(_url("{}".format(collection_name)), headers=utils.get_headers(), data=json_doc)
    if response.status_code == 201:
        return 0, response.status_code, json.loads(response.content)
    else:
        return 1, response.status_code, json.loads(response.content)

def put_document(collection_name, json_doc, etag, database_id):
    """

    Args:
        collection_name: str
            Name of the collection
        json_doc: JSON object
            Document
        etag: long
            Hash_function tag for concurrency management
        database_id:
            Database id

    Returns:
        _: int
            Result code 0-success 1-failure
        _: str
            Result message
        _: JSON Object
            Extra result content in a JSON format

    """
    headers = utils.get_headers()
    headers['If-Match'] = etag
    response = requests.put(_url("{}/{}".format(collection_name, database_id)), headers=headers, data=json_doc)

    if response.status_code == 200:
        return 0, response.status_code, json.loads(response.content)
    else:
        return 1, response.status_code, json.loads(response.content)
