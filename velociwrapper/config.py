import logging
import os
import json
import elasticsearch
from traceback import format_exc

### Default configuration ###

# Don't edit these values directly
# either set them on import with velociwrapper.config.varname = value
# or through the environment VW_VARNAME=value

# Data source node names as a list
# VW_DSN environment variable can be JSON or a comma separated list
dsn = ['localhost']

# Additional parameters to pass to connection Transport class
# if you need a special transport class these parameters mostly 
# get passed to it via the underlying library
connection_params = {}

# Default index to find models
default_index = 'es_model'

# Default number of entries on bulk requests
bulk_chunk_size = 1000

# Default number of matches to return per page
results_per_page = 50

### you should't edit this file at all but definitely don't edit below here! ###


# Set the logger
logger = logging.getLogger('Velociwrapper')

# Check for environment overrides
if os.environ.get('VW_DSN'):
	try:
		dsn = json.loads(os.environ.get('VW_DSN'))
	except:
		logger.debug('VW_DSN was not JSON. Trying to split string')
		dsn = os.environ.get('VW_DSN').split(",")
	
	logger.debug('dsn set from environment')

if os.environ.get('VW_DEFAULT_INDEX'):
	default_index = os.environ.get('VW_DEFAULT_INDEX')
	logger.debug('default_index set from environment')


if os.environ.get('VW_BULK_CHUNK_SIZE'):
	bulk_chunk_size = os.environ.get('VW_BULK_CHUNK_SIZE')
	logger.debug('bulk_chunk_size set from environment')

if os.environ.get('VW_CONNECTION_PARAMS'):
	try:
		connection_params = json.loads( os.environ.get('VW_CONNECTION_PARAMS'))
		logger.debug('connection_params set from environment')
	except:
		logger.warning('Unable to parse VW_CONNECTION_PARAMS from environment. Using default.')
		logger.debug( format_exc() )

if os.environ.get('VW_RESULTS_PER_PAGE'):
	results_per_page = os.environ.get('results_per_page')
	logger.debug('results_per_page set from environment')

# Connect to elastic search
# You can override this to create your own connection (though not from the environment)
from elasticsearch import Elasticsearch
es = Elasticsearch( dsn, **connection_params )
