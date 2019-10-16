# Import flask and template operators
from flask import Flask, render_template
import werkzeug.exceptions as exc
import logging, logging.config
import os
import sys

os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

from data_ingestion import utils, config

s_logger = logging.getLogger('py4j.java_gateway')
s_logger.setLevel(logging.ERROR)
utils.check_folder('logs')
logging.config.fileConfig(os.path.join(BASE_DIR, 'data_ingestion', 'logging.ini'))
logger = logging.getLogger()
logger.info('############### CHECKPOINT')
logger.info('Logger initialized by ./logging.ini file')

# Define the WSGI application object
app = Flask(__name__)
app.config.from_object(config)
app.jinja_env.lstrip_blocks = True
app.jinja_env.trim_blocks = True


# HTTP error handling for non-existing URL or server errors
@app.errorhandler(exc.HTTPException)
def error_handler(error):
    return render_template('error.html', error=error)

from data_ingestion.views import mod_home
from data_ingestion.api import mod_api
app.register_blueprint(mod_home)
app.register_blueprint(mod_api)