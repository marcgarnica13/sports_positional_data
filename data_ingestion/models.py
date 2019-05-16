import pandas as pd
import os
from data_ingestion import utils

pd.set_option('display.max_columns', 500)

def create_new_message(type, id, name, message):
    newMessage = {
        'type': type,
        'id': id,
        'name': name,
        'message': message
    }
    return newMessage

def check_user_input(key):
    return key == 'dshs#input'

class DataImport():

    def __init__(self, mapping, name, message, file_name):
        self.mapping_id = mapping
        self.author = name
        self.import_message = message
        self.data_file_name = file_name
        self.m = utils.get_document_by_id('mappings', self.mapping_id)

    def validate(self):
        ImportFile = {}

        validation_messages = []
        validation_messages.append(create_new_message('text', '', '', 'Import author: ' + self.author))
        validation_messages.append(create_new_message('text', '', '', 'Import message: ' + self.import_message))
        validation_messages.append(create_new_message('text', '', '', 'Import data file: ' + self.data_file_name))
        validation_messages.append(create_new_message('text', '', '', 'Data file mapping: ' + self.m['name']))
        validation_messages.append(create_new_message('text', '', '', 'Data file format: ' + self.m['file']['format']))

        df = pd.read_csv(os.path.join('temp', self.data_file_name), delimiter=self.m['file']['delimiter'], header=self.m['file']['header'])
        data_points = self.m['data_point']

        if (check_user_input(data_points['source_name'])):
            validation_messages.append(create_new_message('inputText', 'source_name', 'source_name', 'Value required: Source name'))
        if (check_user_input(data_points['source_name'])):
            validation_messages.append(create_new_message('inputText', 'source_identifier', 'source_identifier', 'Value required: Source identifier'))

        print('new Import validated')
        return validation_messages

