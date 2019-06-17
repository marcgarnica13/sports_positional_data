import requests
import json
import time
import logging as lg

from data_ingestion import config, utils

def _url(path):
    """ Helper: MongoDB API URL builder
    -----------------------------------
    Parameters
    ----------
    path : str
        Collection and document specific path. Ex: mappings/1 or Games/9
    Returns
    -------
    _ : str
        Complete and function API URL for MongoDB REST API.
    """
    return config.MONGODB_API_URL + path

def get_collection(collection_name):
    """ Get collection from MongoDB API
    -----------------------------------
    Parameters
    ----------
    collection_name : str
        Name of the collection

    Returns
    -------
    collection_list : list(JSON)
        List of JSON document in the MongoDB collection
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
    return collection_list

def get_mongo_api_attributes():
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
    """ Import new document into MongoDb. If the document already exists either
    overwrite it with PATHCH or pass
    ---------------------------------------------------------------------------
    Parameters
    ----------
    collection_name : str
        Name of the collection
    new_doc : JSON object
        Document JSON object
    document_key : str
        Domain primary key attribute of the document
    overwrite : Bool[False]
        Boolean indicating whether the function must patch the document if already
        existing or not.

    Returns
    -------
    _ : int
        Internal status code -> 0: success | 1: failure
    _ : str
        Custom message with the result of the import
    _ : JSON Object
        JSON object with the content of the HTML final request response
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
        if response_code != 0:
            return 4, "Post operation on {} failed".format(collection_name), response_content

    lg.debug("Import done in {} seconds".format(time.time() - start))
    return 0, "", response_content

def get_document(collection_name, document_key, database_attributes=True):
    """ Get document from MongoDB
    ----------------------------------------
    Parameters
    ----------
    collection_name : str
        Name of the collection
    document_key : str
        Domain primary key attribute of the document
    database_attributes : Bool[True]
        Boolean indicating wether to include database specific attributes into the document

    Returns
    -------
    _ : int
        Internal status code -> 0: success | 1: failure
    _ : int
        HTML requests REST status code
    _ : JSON Object
        JSON object with the content of the HTML request response
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
    """ Patch document into MongoDB database
    ----------------------------------------
    Parameters
    ----------
    collection_name : str
        Name of the collection
    json_doc : JSON object
        Document JSON object
    etag:
        Hash function tag for database operations concurreny
    database_id
        Object _id of the existing document to patch

    Returns
    -------
    _ : int
        Internal status code -> 0: success | 1: failure
    _ : int
        HTML requests REST status code
    _ : JSON Object
        JSON object with the content of the HTML request response
    """
    headers = utils.get_headers()
    headers['If-Match'] = etag
    print(headers)
    response = requests.patch(_url("{}/{}".format(collection_name, database_id)), headers=headers, data=json_doc)

    if response.status_code == 200:
        return 0, response.status_code, json.loads(response.content)
    else:
        return 1, response.status_code, json.loads(response.content)

def post_document(collection_name, json_doc):
    """ Post document into MongoDB database
    ----------------------------------------
    Parameters
    ----------
    collection_name : str
        Name of the collection
    json_doc : JSON object
        Document JSON object
    Returns
    -------
    _ : int
        Internal status code -> 0: success | 1: failure
    _ : int
        HTML requests REST status code
    _ : JSON Object
        JSON object with the content of the HTML request response
    """
    response = requests.post(_url("{}".format(collection_name)), headers=utils.get_headers(), data=json_doc)
    print(response.status_code)
    if response.status_code == 201:
        return 0, response.status_code, json.loads(response.content)
    else:
        return 1, response.status_code, json.loads(response.content)

def put_document(collection_name, json_doc, etag, database_id):
    """ Put document into MongoDB database
    ----------------------------------------
    Parameters
    ----------
    collection_name : str
        Name of the collection
    json_doc : JSON object
        Document JSON object
    etag:
        Hash function tag for database operations concurreny
    database_id
        Object _id of the existing document to patch

    Returns
    -------
    _ : int
        Internal status code -> 0: success | 1: failure
    _ : int
        HTML requests REST status code
    _ : JSON Object
        JSON object with the content of the HTML request response
    """
    headers = utils.get_headers()
    headers['If-Match'] = etag
    print(headers)
    response = requests.put(_url("{}/{}".format(collection_name, database_id)), headers=headers, data=json_doc)

    if response.status_code == 200:
        return 0, response.status_code, json.loads(response.content)
    else:
        return 1, response.status_code, json.loads(response.content)
