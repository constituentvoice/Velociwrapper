from elasticsearch import Elasticsearch, NotFoundError, helpers
from datetime import date,datetime
from dateutil import parser
from uuid import uuid4
import json
import types
import copy
import logging
logger = logging.getLogger('Velociwrapper')

# make sure this gets updated!
__version__ = '0.2.15'

dsn = ['localhost']
default_index = 'es_model'
registry = {}
bulk_chunk_size = 1000

class ObjectDeletedError(Exception):
	pass

class NoResultsFound(Exception):
	pass

class relationship(object):
	def __init__(self,ref_model,**kwargs):
		self.ref_model_str = ref_model
		self.ref_model = None

		# reltype is one or many
		self.reltype = 'one'
		if kwargs.get('_type'):
			self.reltype = kwargs.get('_type')
			del kwargs['_type']
		
		self.params = kwargs
	
	def _find_related_class(self,name,cls=None):
		if not cls:
			cls = VWBase
		
		classes = {}
		for sc in cls.__subclasses__():
			if name == sc.__name__:
				return sc
			else:
				possible = self._find_related_class(name,sc)
				if possible and possible.__name__ == name:
					return possible
		
		return None

	
	# returns the relationship lookup values for a given instance
	def get_relational_params(self,cur_inst):
		dict_params = {}
		for k,v in self.params.iteritems():
			dict_params[k] = getattr(cur_inst,k)

		return dict_params

	def get_reverse_params(self,cur_inst,new_obj):
		dict_params = {}

		for k,v in self.params.iteritems():
			# catching of attributeerror maybe unintended consequence
			if new_obj and isinstance(new_obj,VWBase):
				dict_params[k] = getattr(new_obj,v)
			else:
				dict_params[k] = None

	def execute(self, cur_inst):
		
		# first pass we'll need the reference model
		if not self.ref_model:
			self.ref_model = self._find_related_class(self.ref_model_str) 

		if not self.ref_model:
			raise AttributeError('Invalid relatonship. Could not find %s.' % self.ref_model_str )

		c = VWCollection(base_obj=self.ref_model)
		filter_params = {}
		possible_by_id = False
		for k,v in self.params.iteritems():
			column_value = getattr(cur_inst,k)

			# we can't do anything unless there's a value for the column
			# this will allow us to create blank classes properly
			if column_value:
				if v == 'id':
					possible_by_id = column_value
				else:
					if type(column_value) == list:
						or_values = []
						
						# let's be a bit magical
						for item in column_value:
							if isinstance(item,dict) and item.get('id'):
								or_values.append(v + "=" + "'" + item.get('id') + "'") # look for  dictionaries that have 
							elif isinstance(item,basestring):
								or_values.append(v + "=" + "'" + item + "'")
							else:
								raise AttributeError('Unable to parse relationship')


					else:
						filter_params[v] = getattr(cur_inst,k)

		value = None
		
		if not filter_params and possible_by_id:
			if type( possible_by_id ) == list:
				value = c.get_in(possible_by_id)
			else:
				value = c.get(possible_by_id)
		else:
			srch = c.filter_by(**filter_params)

			if self.reltype == 'one':
				try:
					value = srch.one()
				except NoResultsFound:
					pass
			else:
				value = srch.all()

		return value
			
class VWBase(object):
	# connects to ES
	_watch = False
	_needs_update = False
	id = ''
	
	def __init__(self,**kwargs):
		# connect using defaults or override with kwargs

		# relationships should not be executed when called from init (EVAR)
		self._no_ex = True
		
		if kwargs.get('_set_by_query'):
			self._new = False
			self._set_by_query = True
		else:
			self._new = True

		self._needs_update = False
		self._watch = True
		self._es = Elasticsearch( dsn )
		self._deleted = False

		if '__index__' not in dir(self):
			self.__index__ = default_index

		for k in dir(self):
			v = getattr(self,k)
			# skip functions and special variables
			if not isinstance(v,types.MethodType) and not k[0] == '_':

				# check if we were called with a variable. If so set
				if kwargs.get(k):
					v = kwargs.get(k)
					if (isinstance(self.__class__.__dict__.get(k), date) or isinstance(self.__class__.__dict__.get(k),datetime) ) and not (isinstance(v, date) or isinstance(v,datetime)):
						try:
							v = parser.parse(v)

							# convert dates back to date object value (instead of datetime returned by parser.parse)
							# checking "not datetime" because datetime derives from date ("is date" is always true)
							if not isinstance( self.__class__.__dict__.get(k), datetime ):
								v = v.date()
						except:
							pass

				# set the instance variable to the appropriate values
				setattr(self,k,v)

		if 'id' not in kwargs:
			self.id = str(uuid4())

		# make sure we're ready for changes
		self._set_by_query = False
		self._no_ex = False
	
	def __getattribute__(self,name):
		# ok this is much funky
		
		no_ex = False
		try:
			no_ex = super(VWBase,self).__getattribute__('_no_ex')
		except AttributeError:
			pass
	
		v = super(VWBase,self).__getattribute__(name)

		# we want to keep the relationships if set_by_query in the collection so we only execute with direct access
		# (we'll see, it might have an unintended side-effect)
		if isinstance(v,relationship) and not no_ex:
			return v.execute(self)
		else:
			return v

	def __setattr__(self,name,value):
		if '_deleted' in dir(self) and self._deleted:
			raise ObjectDeletedError

		# we need to do some magic if the current value is a relationship
		try:
			currvalue = super(VWBase,self).__getattribute__(name)
		except AttributeError:
			currvalue = None
		
		if isinstance(currvalue,relationship) and not isinstance(value, relationship):
			currparams = currvalue.get_relational_params(self)
			newparams = currvalue.get_reverse_params(self,value)
			
			if isinstance(value,list) and currvalue.reltype == 'many':
				if len(value) > 0:
					for v in value:
						if not isinstance(v,VWBase):
							raise TypeError('Update to %s must be a list of objects that extend VWBase' % name )


					
			elif isinstance(value,VWBase) or value == None:
				pass
			else:
				raise TypeError('Update to %s must extend VWBase or be None' % name)


			for k,v in currparams.iteritems():

				# if left hand value is a list 
				if isinstance( v, list ):
					newlist = []
					# if our new value is a list we should overwrite
					if isinstance(value, list):
						newlist = map( lambda item: getattr(item, k), value )

					# otherwise append
					else:
						# had to reset the list because I can't directly append
						newlist = super(VWBase, self).__getattribute__(k)
						
					object.__setattr__( self, k, newlist )
				# if left hand value is something else
				else:
					# if we're setting a list then check that the relationship type is "many"
					if isinstance(value, list) and currvalue.reltype == 'many':
						# if the length of the list is 0 we will null the value
						if len(value) < 1:
							relation_value = ''
						else:
							# the related column on all items would have to be the same (and really there should only be one but we're going to ignroe that for now)
							relation_value = getattr(value[0],k)

						object.__setattr__( self, k, relation_value )
					else:
						# set the related key to the related key value (v)
						if value:
							object.__setattr__( self, k, v )

		# attribute is NOT a relationship
		else:
								
			if name[0] == '_':
				# special rules for names with underscores.
				# seting the _ values will not trigger an update. 
				if name not in dir(self) or name in ['_set_by_query','_deleted','_watch','_new','_no_ex']:
					object.__setattr__(self,name,value)  # don't copy this stuff. Set it as is

			else:
				object.__setattr__(self,name,copy.deepcopy(value))

				if self._watch:
					object.__setattr__(self,'_needs_update',True)
					object.__setattr__(self,'_watch',False)



	def commit(self):
		# save in the db

		if self._deleted and self.id:
			self._es.delete(id=self.id,index=self.__index__,doc_type=self.__type__)
		else:
			idx = self.__index__
			doc_type = self.__type__

			doc = {}
			for k,v in self.__dict__.iteritems():
				if k[0] == '_' or k == 'id':
					continue

				if isinstance(v,relationship): # cascade commits? # probably not
					continue

				doc[k] = v

			res = self._es.index(index=idx,doc_type=doc_type,id=self.id,body=doc)
			self._watch = True


	def sync(self):
		if self.id:
			try:
				res = self._es.get(id=self.id,index=self.__index__)
				for k,v in res.get('_source').iteritems():
					if self.__class__.__dict__.get(k) and ( isinstance( self.__class__.__dict__.get(k), date ) or isinstance( self.__class__.__dict__.get(k), datetime ) ):
						try:
							v = parser.parse(v)
						except:
							pass

					setattr(self,k,v)

				self._new = False

			except NotFoundError:
				# not found in elastic search means we should treat as new
				self._new = True
		else:
			raise AttributeError('Object is not committed')


	def delete(self):
		if self.id:
			self._deleted = True

	# to dict is for overriding. _create_source_document() should never be overridden!
	def to_dict(self):
		return self._create_source_document(datetime_format='%Y-%m-%d %H:%M:%S')  # to_dict should use true ISO format without the T

	def _create_source_document(self, **kwargs):
		output = {}
		date_format = kwargs.get('date_format','%Y-%m-%d')
		datetime_format = kwargs.get('datetime_format','%Y-%m-%dT%H:%M:%S')
		for k,v in self.__dict__.iteritems():
			if k[0] != '_':
				if isinstance(v,datetime):
					# output[k] = v.isoformat()
					output[k] = v.strftime(datetime_format)
				elif isinstance(v,date):
					output[k] = v.strftime(date_format)
				else:
					output[k] = copy.deepcopy(v)

		return output

	def more_like_this(self,**kwargs):
		c = VWCollection(base_obj=self.__class__)
		return c.get_like_this(self.id).all(**kwargs)

class VWCollection(object):
	def __init__(self,items=[],**kwargs):
		self.bulk_chunk_size = bulk_chunk_size

		if kwargs.get('bulk_chunk_size'):
			self.bulk_chunk_size = kwargs.get('bulk_chunk_size')

		self.results_per_page = 50
		self._sort = []
		
		if kwargs.get('results_per_page'):
			self.results_per_page = kwargs.get('results_per_page')

		if kwargs.get('base_obj'):
			self.base_obj = kwargs.get('base_obj')
		else:
			try:
				self.base_obj = self.__class__.__model__
			except AttributeError:
				raise AttributeError('Base object must contain a model or pass base_obj')

		self._es = Elasticsearch( dsn )

		if '__index__' in dir(self.base_obj):
			idx = self.base_obj.__index__
		else:
			idx = default_index

		self._search_params = []
		self._raw = {}
		self.idx = idx
		self.type = self.base_obj.__type__
		self._special_body = {}
		self._items = items # special list of items that can be committed in bulk

	def _create_obj_list(self,es_rows):
		retlist = []
		for doc in es_rows:
			if doc.get('_source'):
				retlist.append( self._create_obj(doc) )

		return retlist

	def _create_obj(self,doc):
		src = doc.get('_source')
		src['_set_by_query'] = True
		src['id'] = doc.get('_id')
		return self.base_obj(**src)

	def search(self,q):
		self._search_params.append(q)
		return self

	# setup a raw request
	def raw(self, raw_request):
		self._raw = raw_request
		return self

	def _do_search(self,q):
		results = self._es.search(index=self.idx,q=q,doc_type=self.type)
		return self._create_obj_list( results.get('hits').get('hits') )

	def filter_by( self, condition = 'and',**kwargs ):
		groups = []
		for k,v in kwargs.iteritems():
			groups.append( str(k) + ':"' + str(v) + '"')

		conditions = {
			'and': self.and_,
			'or': self.or_
		}

		query = conditions[condition.lower()](*groups)

		if condition == 'and':
			query = self.and_(*groups)

		return self.search( query )

	def or_(self,*args):
		return ' OR '.join(args)

	def and_(self,*args):
		return ' AND '.join(args)

	def get(self,id):
		try:
			return self._create_obj( self._es.get(index=self.idx,doc_type=self.type,id=id) )
		except:
			return None

	def get_in(self, ids):
		
		if len(ids) > 0: # check for ids. empty list returns an empty list (instead of exception)
			res = self._es.mget(index=self.idx,doc_type=self.type,body={'ids':ids})
			if res and res.get('docs'):
				return self._create_obj_list( res.get('docs') )

		return []

	def get_like_this(self,doc_id):
		res = self._es.mlt(index=self.idx,doc_type=self.type,id=doc_id )
		if res and res.get('docs'):
			return self._create_obj_list( res.get('docs') )
		else:
			return []

	def sort(self, **kwargs ):
		for k,v in kwargs.iteritems():
			v = v.lower()
			if v not in ['asc','desc']:
				v = 'asc'

			self._sort.append( '%s:%s' % (k,v) )
		return self

	def clear_previous_search( self ):
		self._raw = {}
		self._search_params = []

	def _create_search_params( self ):
		q = {
			'index': self.idx,
			'doc_type': self.type
		}

		if self._raw:
			q['body'] = self._raw
		elif len(self._search_params) > 0:
			q['q'] = self.and_(*self._search_params)

		else:
			q['body'] = {'query':{'match_all':{} } }
		

		# this allows for searching along with geo and range queries
		if self._special_body:
			q['body'] = self._special_body
		
		logger.debug(json.dumps(q))
		return q


	def count(self):
		params = self._create_search_params()
		resp = self._es.count(**params)
		return resp.get('count')


	def __len__(self):
		return self.count()

	def all(self,**kwargs):

		params = self._create_search_params()
		if not params.get('size'):
			params['size'] = self.results_per_page

		if kwargs.get('results_per_page') != None:
			kwargs['size'] = kwargs.get('results_per_page')
			del kwargs['results_per_page']

		if kwargs.get('start') != None:
			kwargs['from_'] = kwargs.get('start')
			del kwargs['start']
		
		logger.debug( json.dumps( self._sort )  )

		params.update(kwargs)
		if len(self._sort) > 0:
			if params.get('sort') and isinstance(params['sort'], list):
				params['sort'].extend(self._sort)
			else:
				params['sort'] = self._sort
		
		if params.get('sort'):
			if isinstance(params['sort'], list):
				params['sort'] = ','.join(params.get('sort'))
			else:
				raise TypeError('"sort" argument must be a list')
		
		results = self._es.search( **params )
		rows = results.get('hits').get('hits')

		return self._create_obj_list( rows )

	def one(self,**kwargs):
		results = self.all(results_per_page=1)
		try:
			return results[0]
		except IndexError:
			raise NoResultsFound('No result found for one()')

	# builds query bodies
	def _build_body( self, **kwargs ):
		if not self._special_body:
			self._special_body = { "query": {} }
		
		if kwargs.get('filter'):
			if not self._special_body.get('query').get('filtered'):
				current_q = self._special_body.get('query')

				self._special_body['query']['filtered'] = { 'query': current_q, 'filter':{}  }
			
			

			self._special_body['query']['filtered']['filter'].update( kwargs.get('filter'))
		elif kwargs.get('query'):
			if self._special_body.get('query').get('filtered'):
				self._special_body.get('query').get('filtered').get('query').update(kwargs.get('query'))

			else:
				self._special_body.get('query').update(kwargs.get('query'))

	def _special_body_is_filtered(self):
		return (self._special_body and self._special_body.get('query').get('filtered'))

	def range(self, field, **kwargs):
		q = {'range': { field: kwargs } }
		if self._special_body_is_filtered():
			d = {'filter': q }
		else:
			d = {'query': q }

		self._build_body(**d)
		return self

	def search_geo(self, field, distance, lat, lon):
		return self.raw( {
			"query": {
				"filtered": { 
					"query": {
						"match_all": {}
					},
					"filter": {
						"geo_distance": {
							"distance": distance,
							field: [ lon, lat ]
						}
					}
				}
			}
		})

	
	def delete(self, **kwargs):
		params = self._create_search_params()
		params.update(kwargs)
		self._es.delete_by_query( **params )


	def delete_in(self, ids):
		if not isinstance(ids, list):
			raise TypeError('argument to delete in must be a list.')

		bulk_docs = []
		for i in ids:
			this_id = i
			this_type = self.base_obj.__type__
			this_idx = self.idx
			if isinstance(i, VWBase):
				this_id = i.id
				this_type = i.__type__
				try:
					this_idx = i.__index__
				except AttributeError:
					pass

			bulk_docs.append( {'_op_type': 'delete', '_type': this_type, '_index': this_idx, '_id': this_id } )

		return helpers.bulk( self._es, bulk_docs, chunk_size=self.bulk_chunk_size)
	
	# commits items in bulk
	def commit(self, callback=None):
		bulk_docs = []

		if callback:
			if not callable(callback):
				raise TypeError('Argument 2 to commit() must be callable')

		for i in self._items:
			if callback:
				i = callback(i)

			this_dict = {}
			this_id = ''
			this_idx = self.idx
			this_type = self.base_obj.__type__
			if isinstance(i, VWBase):
				this_dict = i._create_source_document()
				this_type = i.__type__
				this_id = i.id
				try:
					this_idx = i.__index__
				except AttributeError:
					pass

			elif isinstance(i,dict):
				this_dict = i
				this_id = i.get('id')
			
			else:
				raise TypeError( 'Elments passed to the collection must be type of "dict" or "VWBase"' )
			
			if not this_id:
				this_id = str(uuid4())

			bulk_docs.append( {'_op_type': 'index', '_type': this_type, '_index': this_idx, '_id': this_id, '_source': this_dict } )

		return helpers.bulk(self._es,bulk_docs,chunk_size=self.bulk_chunk_size)
