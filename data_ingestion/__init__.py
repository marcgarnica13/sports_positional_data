# Import flask and template operators
from flask import Flask, render_template
import logging, logging.config
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

import utils, config

s_logger = logging.getLogger('py4j.java_gateway')
s_logger.setLevel(logging.ERROR)
utils.check_folder('logs')
logging.config.fileConfig(os.path.join(BASE_DIR, 'data_ingestion', 'logging.ini'))
logger = logging.getLogger()
logger.info('############### CHECKPOINT')
logger.info('Logger initialized by ./logging.ini file')


