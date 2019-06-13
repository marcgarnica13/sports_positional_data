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
    jsonify

from data_ingestion import utils, mongo_api, config
from data_ingestion.forms import UploadForm
from data_ingestion.models import DataImport

mod_home = Blueprint('home', __name__, url_prefix='/')

@mod_home.route('/', methods = ['GET', 'POST'])
def home():
    utils.delete_temp_folder()

    form = UploadForm(CombinedMultiDict((request.files, request.form)))
    print(request.form)
    form.selected_mapping.choices = [(m['_id'], m['name']) for m in mongo_api.get_collection(config.MAPPINGS_COLLECTION_NAME)]

    if request.method == 'POST':
        case = request.form['button']
        if case == 'data_import':
            lg.debug("New data import")
            print(form.selected_mapping.data)
            if form.validate_on_submit():
                data_file_name = secure_filename(form.data_file.data.filename)
                utils.save_temp_file(request.files['data_file'])
                return redirect(
                    url_for("home.validate",
                            selected_mapping=form.selected_mapping.data,
                            full_name=form.full_name.data,
                            message=form.message.data,
                            data_file_name=data_file_name))
        elif case == 'mapping_import':
            lg.debug("Starting new mapping import")
            if form.mapping_file.data is None:
                form.mapping_file.errors = tuple(['You missed the new mapping json file!'])
            else:
                new_mapping = json.load(request.files['mapping_file'])
                result_code, result_message = mongo_api.import_new_document(config.MAPPINGS_COLLECTION_NAME, new_mapping, config.MAPPINGS_KEY, overwrite=True)
                if result_code != 0:
                    return render_template("error.html", error=result_message)
                else:
                    return render_template("success.html", message="New mapping added correctly")
        elif case == 'download':
            lg.debug("Processing requests to download mapping")
            selected_id = form.selected_mapping.data
            if selected_id == '-1':
                return jsonify(utils.get_default_mapping()), 200
            code, response_code, response = mongo_api.get_document(config.MAPPINGS_COLLECTION_NAME, document_key=selected_id, database_attributes=False)
            if code == 0:
                return jsonify(response),200
            else:
                return render_template("error.html", error="{} : {}".format(response_code, response['_error']['message']))
    return render_template("home.html", form=form)

@mod_home.route('/data_validation', methods = ['GET','POST'])
def validate():
    newImport = DataImport(request.args['selected_mapping'],
                           request.args['full_name'],
                           request.args['message'],
                           request.args['data_file_name'])
    correct, messages, metadata = newImport.validate()
    if not correct:
        return render_template("error.html", error=messages)
    else:
        return render_template("validation.html", messages=messages, args=request.args, metadata=metadata)

@mod_home.route('/data_uploader', methods = ['POST'])
def data_upload():
    print('data uploader')
    newImport = DataImport(request.args['selected_mapping'],
                           request.args['full_name'],
                           request.args['message'],
                           request.args['data_file_name']
                           )
    report = newImport.run()
    return render_template("error.html", error=report)

@mod_home.route('/mapping_uploader', methods = ['POST'])
def mapping_upload():
    print('mapping uploader')
