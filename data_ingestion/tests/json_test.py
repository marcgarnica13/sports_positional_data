import os
import json
import logging as lg
from pyspark.sql import functions
from data_ingestion.file_processor import json_processor
from data_ingestion import utils

bulkImport = utils.load_pickle_object('nba_movement_bulk')
for collection, json_doc_set in bulkImport.items():
    lg.debug(json.dumps(json_doc_set, indent=4))
