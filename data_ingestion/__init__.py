# Import flask and template operators
from flask import Flask, render_template
import werkzeug.exceptions as exc
import logging, logging.config

from data_ingestion import utils, config

utils.check_folder('logs')
logging.config.fileConfig('logging.ini')
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
app.register_blueprint(mod_home)