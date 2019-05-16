# Import flask and template operators
from flask import Flask, render_template
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

# Sample HTTP error handling
@app.errorhandler(404)
def not_found(error):
    return render_template('404.html'), 404

from data_ingestion.views import mod_home
app.register_blueprint(mod_home)