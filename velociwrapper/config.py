import logging
import os

# Default configuration

# Don't edit these values directly
# either set them on import with velociwrapper.config.varname = value
# or through the environment VW_VARNAME=value

# Default index to find models
default_index = 'es_model'

# Default number of entries on bulk requests
bulk_chunk_size = 1000

# Default number of matches to return per page
results_per_page = 50

# Should we enforce strict types
strict_types = False

""" you should't edit this file at all but definitely don't edit below here! """

# Set the logger
logger = logging.getLogger('Velociwrapper')

if os.environ.get('VW_DEFAULT_INDEX'):
    default_index = os.environ.get('VW_DEFAULT_INDEX')
    logger.debug('default_index set from environment')

if os.environ.get('VW_BULK_CHUNK_SIZE'):
    try:
        bulk_chunk_size = int(os.environ.get('VW_BULK_CHUNK_SIZE'))
        logger.debug('bulk_chunk_size set from environment')
    except ValueError:
        logger.warn('invalid value for VW_BULK_CHUNK_SIZE, expected integer. Using default')

if os.environ.get('VW_STRICT_TYPES'):
    try:
        strict_types = int(os.environ.get('VW_STRICT_TYPES'))

        logger.debug('strict_types set from environment')
    except ValueError:
        logger.warning('Invalid value for VW_STRICT_TYPES, expected 0 or 1. Using default')

if os.environ.get('VW_RESULTS_PER_PAGE'):
    try:
        results_per_page = int(os.environ.get('VW_RESULTS_PER_PAGE'))
        logger.debug('results_per_page set from environment')
    except ValueError:
        logger.warning('Invalid value for VW_RESULTS_PER_PAGE. Expected integer. Using default.')
