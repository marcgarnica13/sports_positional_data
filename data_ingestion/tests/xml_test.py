import os
from data_ingestion.file_processor import xml_processor, csv_processor

processor = csv_processor.CSVProcessor(file_path='large_kinexon.csv', header='True', delimiter=';')

processor.set_processor_arrays(['1'],['1'],['1'],['1'],['1'],['1'])
processor.process()