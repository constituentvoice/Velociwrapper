from datetime import date,datetime
import types
from .config import logger
import re
from traceback import format_exc

# check the python type and return the appropriate ESType class
def create_es_type(value):
	
	# check if we're already an es type
	try:
		if value.__metaclass__ == ESType:
			return value
	except:
		pass

	if type(value) == str or type(value) == unicode:
		# strings could be a lot of things
		# try to see if it might be a date
			
		# dateutil isn't good at determining if we have a date (ok at parsing if we know there's a date). To that end we'll only accept a couple of valid formats
		test_date = value.strip()
		test_date = re.sub("(?:Z|\s*[\+\-]\d\d:?\d\d)$", '', test_date)

		try:
			test_date = datetime.strptime( test_date, '%Y-%m-%d %H:%M:%S' )
			return DateTime( test_date )

		except ValueError:
			try:
				test_date = datetime.strptime( test_date, '%Y-%m-%dT%H:%M:%S')
				return DateTime(test_date)
			except ValueError:
				try:
					test_date = datetime.strptime(test_date,'%Y-%m-%d')
					return Date(test_date.date())
				except ValueError:
					pass

		# see if it might be an ip address
		try:
			matches = re.search( '^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$', value )
			if matches:
				valid_ip = True
				for g in matches.groups():
					g = int(g)
					if g < 1 or g > 254:
						# nope
						valid_ip = False
				
				if valid_ip:
					return IP(value)
		except:
			pass

		if type(value) == unicode:
			if value.isnumeric():
				return Number(value)
			else:
				return String(value)

		return String(value)

	if type(value) == int:
		return Integer(value)
	
	if type(value) == bool:
		return Boolean(value)

	if type(value) == long:
		return Long(value)
	
	if type(value) == float:
		return Float(value)

	if isinstance(value,date):
		return Date(value)

	if isinstance(value,datetime):
		return DateTime(value)

	# if here, just return the value as is
	return value

# this is to determine if the field should be analyzed 
# based on type and settings. Used a lot to determine whether to use
# term filters or matches
# works with estypes and non-estypes
def is_analyzed(value):
	analyzed = True
	check_defaults = False
	try:
		if value.__metaclass__ == ESType:
			if isinstance(value, String):
				analyzed = value.es_args().get(analyzed)
				if analyzed == None:
					analyzed = True

			else:
				analyzed = False
		else:
			check_defaults = True
	except AttributeError:
		check_defaults = True


	if check_defaults:
		analyzed = False
		if isinstance(value, object): # this likely should't happen
			analyzed = True
		else:
			checklist = []
			if isinstance(value, list):
				checklist = value
			else:
				checklist = [value]

			for item in checklist:
				if isinstance(value, str) or isinstance(value,unicode):
					analyzed = True
					break

	return analyzed

class ESType(type):
	def __new__(cls,clsname,bases,dct):
		# default accepted properties on various classes
		# the values are set by ES and are only here for completeness
		dct['__es_properties__'] = {
			'Any': {
				'index_name': '',
				'store': False,
				'boost': '1.0',
				'null_value': None,
				'include_in_all': True,
				'doc_values': False,
				'fielddata': {},
				'copy_to': '',
				'similarity': 'default',
				'fields': {},
				'meta_': {}
			},
			'String': {
				'analyzed': True,
				'norms': False,
				'index_options': None,
				'analyzer': 'default',
				'index_analyzer': 'default',
				'search_analyzer': 'default',
				'ignore_above': 'default',
				'position_offset_gap': 0,
				'value_': '',
				'boost_': '1.0'
			},
			'Number': {
				'type_': 'float',
				'index_': 'no',
				'precision_step': 16,
				'ignore_malformed': False,
				'coerce': True
			},
			'Integer': {
				'type_': 'float',
				'index_': 'no',
				'precision_step': 16,
				'ignore_malformed': False,
				'coerce': True
			},
			'Long': {
				'type_': 'long',
				'index_': 'no',
				'precision_step': 16,
				'ignore_malformed': False,
				'coerce': True
			},
			'Date': {
				'format': 'dateOptionalTime',
				'precision_step': 16,
				'ignore_malformed': False
			},
			'DateTime': {
				'format': 'dateOptionalTime',
				'precision_step': 16,
				'ignore_malformed': False
			},
			'Binary': {
				'compress': False,
				'compress_threshold': -1
			},
			'IP': {
				'precision_step': 16
			},
			'GeoPoint': {
				'lat_lon': False,
				'geohash': False,
				'geohash_precision': 12,
				'geohash_prefix': False,
				'validate': False,
				'validate_lat': False,
				'validate_lon': False,
				'normalize': True,
				'normalize_lat': False,
				'normalize_lon': False,
				'precision_step': 16
			},
			'GeoShape': {
				'tree': 'geohash',
				'tree_levels': '',
				'distance_error_pct': 0.5
			}
			# attachment not specified because it has no other args
		}

		def get_prop_dict(self):
			prop_dict = { "type": self.__class__.__name__.lower() }

			valid = []
			for obj in self.__class__.mro():
				if obj.__name__ in self.__es_properties__:
					valid.extend( list( self.__es_properties__.get(obj.__name__) ) )

			valid.extend( list( self.__es_properties__.get('Any') ) )

			for k in dir(self):
				if k in valid:
					v = getattr(self,k)

					keyname = k
					if k[ len(k) - 1 ] == "_":
						if k in ['meta_','value_','boost_']:
							keyname = '_' + k[0:len(k) - 1]
						else:
							keyname = k[ 0:len(k) - 1]

					elif k == 'analyzed':
						keyname = 'index'
						if v or v == None:
							v = 'analyzed'
						else:
							v = 'not_analyzed'
					
					if v != None:	
						prop_dict[keyname] = v

			return prop_dict

		# for recreating the arguments in a new instance
		def get_es_arguments(self):
			arg_dict = {}
			for k in dir(self):
				if k in self.__es_properties__:
					v = getattr(self,k)
					arg_dict[k] = v
			return arg_dict


		dct['prop_dict'] = get_prop_dict	
		dct['es_args'] = get_es_arguments
		
		return super(ESType,cls).__new__(cls,clsname,bases,dct)

	def __call__( cls, *args, **kwargs ):
		
		# we have to split kw args going to the base class
		# and args that are for elastic search
		# annoying but not a big deal
		base_kwargs = {}
		es_kwargs = {}
		_name_ = cls.__name__
		
		valid = []
		for obj in cls.mro():
			if obj.__name__ in cls.__es_properties__:
				valid.extend( list( cls.__es_properties__.get(obj.__name__) ) )

		valid.extend( list( cls.__es_properties__.get('Any') ) )

		for k,v in kwargs.iteritems():
			if k in valid:
				es_kwargs[k] = v
			else:
				base_kwargs[k] = v
		
		# fix for datetime calls. I really dont like this but I can't seem
		# to hook it anywhere

		if len(args) == 1:
			a = args[0]
			if cls == DateTime and isinstance(a, datetime):
				args = [a.year, a.month, a.day, a.hour, a.minute, a.second,a.microsecond, a.tzinfo]
			elif cls == Date and isinstance(a,date):
				args = [a.year,a.month,a.day]
	
		inst = super(ESType,cls).__call__(*args,**base_kwargs)
		
		for k,v in es_kwargs.iteritems():
			setattr(inst, k, v ) # testing

		return inst

# converts strings to unicode
class String(unicode):
	__metaclass__ = ESType
	
class Number(float):
	__metaclass__ = ESType
	precision_step = 8
	coerce_ = True

class Float(Number):
	type_ = 'float'

class Double(Number):
	__metaclass__ = ESType
	type_ = 'double'
	precision_step = 16

class Integer(int):
	__metaclass__ = ESType
	precision_step = 8
	coerce_ = True
	type_ = 'integer'

class Long(long):
	__metaclass__ = ESType
	coerce_ = True
	type_ = 'long'
	precision_step = 16

class Short(Integer):
	type_ = 'short'

class Byte(Number):
	__metaclass__ = ESType
	type_ = 'byte'
	precision_step = 2147483647 # wat? (its from the ES docs)

class TokenCount(Number):
	__metaclass__ = ESType
	analyzer = 'standard'

class DateTime(datetime):
	__metaclass__ = ESType
	precision_step = 16
	ignore_malformed = False

	def date(self):
		value = super(DateTime,self).date()
		return Date(value)

	# TODO allow format changes
	# for now just does default

	def __init__(self,*args,**kwargs):
		try:
			if isinstance(args[0], datetime):
				d = args[0]
				args = [d.year, d.month, d.day, d.hour, d.minute, d.second]
				if d.tzinfo:
					kwargs['tzinfo'] = d.tzinfo
		except:
			pass

		super(DateTime, self).__init__(*args,**kwargs)

class Date(date):
	__metaclass__ = ESType
	precision_step = 16
	ignore_malformed = False

	def __init__(self, *args,**kwargs):
		try:
			if isinstance(args[0], date):
				d = args[0]
				args = [d.year, d.month, d.day]
		except:
			pass

		super(Date, self).__init__(*args,**kwargs)

class Boolean(int):
	# can't extend bool so ... whatever
	__metaclass__ = ESType

class Binary(object):
	__metaclass__ = ESType
	compress = False
	compress_threshold = -1

class IP(String):
	__metaclass__ = ESType

class GeoPoint(object):
	__metaclass__ = ESType
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

class GeoShape(object):
	__metaclass__ = ESType
	tree = 'geohash'
	precision = 'meters'
	
	# TODO - do we want to internally implement all the GeoJSON that goes along with this?

class Attachment(object):
	__metaclass__ = ESType
