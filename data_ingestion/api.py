import json
import logging as lg
from werkzeug.datastructures import CombinedMultiDict
from werkzeug.utils import secure_filename

from flask import \
    render_template,\
    request,\
    url_for,\
    redirect,\
    Blueprint,\
    session,\
    jsonify

import utils, mongo_api, config
from forms import UploadForm
from models import DataImport

mod_api = Blueprint('api', __name__, url_prefix='/api')

# TODO: 4 main links: Get mappings list, get mapping <id>, translate, ingest

@mod_api.route('/mappings', methods = ['GET', 'POST'])
def mappings():
    mappings = [{ 'mapping_id': m['_id'], 'mapping_name': m['name'] } for m in mongo_api.get_collection(config.MAPPINGS_COLLECTION_NAME)]
    return jsonify(mappings)

@mod_api.route('/mappings/<mapping_id>', methods = ['GET'])
def mapping(mapping_id):
    code, status, response = mongo_api.get_document(config.MAPPINGS_COLLECTION_NAME, mapping_id, database_attributes=False)
    if not code:
        return jsonify(mapping=response)
    else:
        response = jsonify({'error': 'Error in the server',
                            'message': 'The request for the specific mapping was not successful',
                            'request status code': status})
        response.status_code = 500

        return response