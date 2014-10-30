# Velociwrapper tools for managing indexes, types and mappings
from elasticsearch import Elasticsearch, NotFoundError,helpers
from datetime import date,datetime
from dateutil import parser
from uuid import uuid4
import json
import types
import copy
from .config import dsn,default_index,bulk_chunk_size,logger
from . import relationship

class ESType(object):
	store = False
	index = 'analyzed'
	term_vector = 'no'
	boost = '1.0'
	include_in_all = True
	similarity = 'default'
	fielddata = {}
	fields = {} # multifield functionality
	_meta = {} # could be terribly useful

class String(ESType):
	pass

class Number(ESType):
	precision_step = 8
	_coerce = True

class Float(Number):
	_type = 'float'

class Double(Number):
	_type = 'double'
	precision_step = 16

class Integer(Number):
	_type = 'integer'

class Long(Number):
	_type = 'long'
	precision_step = 16

class Short(Number):
	_type = 'short'

class Byte(Number):
	_type = 'byte'
	precision_step = 2147483647 # wat? (its from the ES docs)

class TokenCount(Number):
	analyzer = 'standard'

class Date(ESType):
	precision_step = 16
	ignore_malformed = False

	# TODO allow format changes
	# for now just does default

class Boolean(ESType):
	pass

class Binary(ESType):
	compress = False
	compress_threshold = -1

class IP(ESType):
	pass

class GeoPoint(ESType):
	lat_lon = False
	geohash = False
	geohash_precision = None # use default
	geohash_prefix = False
	validate = False
	validate_lat = False
	validate_lon = False
	normalize = True
	normalize_lat = False
	normalize_lon = False


class GeoShape(ESType):
	tree = 'geohash'
	precision = 'meters'
	
	# TODO - do we want to internally implement all the GeoJSON that goes along with this?

class Attachment(ESType):
	pass







class Mapper
