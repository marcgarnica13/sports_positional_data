import os
from data_ingestion.file_processor import json_processor

proc = json_processor.JSONProcessor(
    file_path='nba_movement.json',
    time_format='ms')