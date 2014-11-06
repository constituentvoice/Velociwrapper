from datetime import date,datetime
import types
from .config import logger

class ESType(type):
	def __new__(cls,clsname,bases,dct):

		dct['__es_properties__'] = {
			'store': False,
			'analyzed': True,
			'term_vector': 'no',
			'boost': '1.0',
			'include_in_all':True,
			'similarity':'default',
			'fielddata':None,
			'fields':None,
			'meta_':None,
			'precision_step':None,
			'coerce_':None,
			'type_':None,
			'analyzer':None,
			'ignore_malformed':None,
			'compress':None,
			'compress_threshold':None,
			'lat_lon':None,
			'geohash':None,
			'geohash_precision':None,
			'geohash_prefix':None,
			'validate':None,
			'validate_lat':None,
			'validate_lon':None,
			'normalize':None,
			'normalize_lat':None,
			'normalize_lon':None,
			'tree':None
		}

		def get_prop_dict(self):
			prop_dict = { "type": self.__class__.__name__.lower() }
			for k in dir(self):
				if k in self.__es_properties__:
					v = getattr(self,k)
					if not v:
						v = self.__es_properties__.get(k)

					keyname = k
					if k[ len(k) - 1 ] == "_":
						if k == 'meta_':
							keyname = '_meta'
						else:
							keyname = k[ 0:len(k) - 1]

					elif k == 'analyzed':
						keyname = 'index'
						if v:
							v = 'analyzed'
						else:
							v = 'not_analyzed'
					
					prop_dict[keyname] = v

			return prop_dict

		dct['prop_dict'] = get_prop_dict	
		
		return super(ESType,cls).__new__(cls,clsname,bases,dct)

		
	def __call__( self, *args, **kwargs ):
		# set options passed as keyword args
		# stupid that i have to list this again >:-o
				
		deletes = []
		for k,v in kwargs.iteritems():
			if k in self.__es_properties__:
				setattr(self,k,v)
				deletes.append(k)

		# set defaults
		for k,v in self.__es_properties__.iteritems():
			try:
				val = getattr(self,k)
			except AttributeError:
				if v != None:
					setattr(self,k,v)

		for d in deletes:
			del kwargs[d]

		return super(ESType,self).__call__(*args,**kwargs)

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

class Date(datetime):
	__metaclass__ = ESType
	precision_step = 16
	ignore_malformed = False

	# TODO allow format changes
	# for now just does default

class Boolean(int):
	# can't extend bool so ... whatever
	__metaclass__ = ESType

class Binary(object):
	__metaclass__ = ESType
	compress = False
	compress_threshold = -1

class IP(object):
	__metaclass__ = ESType

	def __init__(self,ip="0.0.0.0"):
		self.ip = ip
	
	def __str__(self):
		return unicode( self.ip )

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
