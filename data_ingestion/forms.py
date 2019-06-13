from flask_wtf import FlaskForm
from wtforms import StringField, SelectField
from wtforms.validators import  DataRequired
from flask_wtf.file import FileField, FileRequired, FileAllowed


class UploadForm(FlaskForm):
    ''' Uploading a new data file to the repository form'''
    full_name = StringField('full_name', [DataRequired(message=('Include your name or some indicator of the data import'))])
    message = StringField('message', [DataRequired(message='Shortly describe the data you are importing')])
    selected_mapping = SelectField('selected_mapping', coerce=str, validators=[DataRequired(message="Select the mapping schema")])
    data_file = FileField('data_file', [FileRequired(message='You missed the data file!')])
    mapping_file = FileField('mapping_file')