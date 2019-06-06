from werkzeug.datastructures import CombinedMultiDict
from werkzeug.utils import secure_filename

from flask import \
    render_template,\
    request,\
    url_for,\
    redirect,\
    Blueprint

from data_ingestion import utils
from data_ingestion.forms import UploadForm
from data_ingestion.models import DataImport

mod_home = Blueprint('home', __name__, url_prefix='/')

@mod_home.route('/', methods = ['GET', 'POST'])
def home():
    utils.delete_temp_folder()
    form = UploadForm(CombinedMultiDict((request.files, request.form)))
    form.selected_mapping.choices = utils.get_mappings_collection()

    if request.method == 'POST':
        if form.validate_on_submit():
            data_file_name = secure_filename(form.data_file.data.filename)
            utils.save_temp_file(request.files['data_file'])
            return redirect(
                url_for("home.validate",
                        selected_mapping=form.selected_mapping.data,
                        full_name=form.full_name.data,
                        message=form.message.data,
                        data_file_name=data_file_name))
    return render_template("home.html", form=form)

@mod_home.route('/data_validation', methods = ['GET','POST'])
def validate():
    newImport = DataImport(request.args['selected_mapping'],
                           request.args['full_name'],
                           request.args['message'],
                           request.args['data_file_name'])
    correct, messages = newImport.validate()
    if not correct:
        return redirect(url_for("home.home"))
    else:
        return render_template("validation.html", messages=messages, args=request.args)

@mod_home.route('/data_uploader', methods = ['POST'])
def data_upload():
    print('data uploader')
    newImport = DataImport(request.args['selected_mapping'],
                           request.args['full_name'],
                           request.args['message'],
                           request.args['data_file_name'],
                           True)
    valid, messages = newImport.validate()
    if valid:
        report = newImport.run()
        return render_template("error.html", error=report)
    else:
        return render_template('error.html', error='Import is not correct')
