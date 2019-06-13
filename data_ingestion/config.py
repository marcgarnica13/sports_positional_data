from pyspark import SparkConf

# Statement for enabling the development environment
DEBUG=False
# Define the application directory
import os
BASE_DIR = os.path.abspath(os.path.dirname(__file__))

# Define the database - we are working with
DATABASE_CONNECT_OPTIONS = {}

# Application threads. A common general assumption is
# using 2 per available processor cores - to handle
# incoming requests using one and performing background
# operations using the other.
THREADS_PER_PAGE = 2

SPARK_CONF = SparkConf()
SPARK_CONF.set('spark.logConf', 'true')
SPARK_CONF.set(
    'spark.jars.packages',
    'com.databricks:spark-xml_2.11:0.5.0')

# Enable protection agains *Cross-site Request Forgery (CSRF)*
CSRF_ENABLED = True
CSRF_SESSION_KEY=os.urandom(32)
SECRET_KEY=os.urandom(32)

MONGODB_API_URL= 'http://localhost:5000/'
MAPPINGS_COLLECTION_NAME = 'mappings'
MAPPINGS_KEY = 'name'
