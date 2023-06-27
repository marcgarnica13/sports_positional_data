from flask import Flask, render_template
import config
import werkzeug.exceptions as exc
from views import mod_home
from api import mod_api

# Define the WSGI application object
app = Flask('Data Ingestion'
            '')
app.config.from_object(config)
app.jinja_env.lstrip_blocks = True
app.jinja_env.trim_blocks = True

# HTTP error handling for non-existing URL or server errors
@app.errorhandler(exc.HTTPException)
def error_handler(error):
    return render_template('error.html', error=error)

app.register_blueprint(mod_home)
app.register_blueprint(mod_api)

if __name__ == '__main__':
      app.run(host='127.0.0.1', port=5001)

