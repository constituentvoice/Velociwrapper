from elasticsearch import Elasticsearch, NotFoundError
from datetime import date,datetime
from dateutil import parser
from uuid import uuid4
import json
import types

dsn = ['localhost']
default_index = 'es_model'

class ObjectDeletedError(Exception):
	pass

class VWBase(object):
	# connects to ES
	_watch = False
	_needs_update = False
	id = ''

	def __init__(self,**kwargs):
		# connect using defaults or override with kwargs
		if kwargs.get('_set_by_query'):
			self._new = False
			self._set_by_query = False
		else:
			self._new = True

		self._needs_update = False
		self._watch = True
		self._es = Elasticsearch( dsn )

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
						except:
							pass

				# set the instance variable to the appropriate values
				setattr(self,k,v)

		if 'id' not in kwargs:
			self.id = str(uuid4())

	def __getattr__(self,name):
		if self.__dict__.get('_deleted'):
			raise ObjectDeletedError

		return self.__dict__.get(name)

	def __setattr__(self,name,value):
		if '_deleted' in dir(self) and self._deleted:
			raise ObjectDeletedError

		if name[0] == '_':
			# special rules for names with underscores. They can be set once but then remain
			# seting the _ values will not trigger an update
			if name not in dir(self):
				object.__setattr__(self,name,value)
		else:
			object.__setattr__(self,name,value)

			if name != '_watch' and name != '_needs_update' and self._watch:
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

	def to_dict(self):
		output = {}
		for k,v in self.__dict__.iteritems():
			if k[0] != '_':
				if isinstance(v,date) or isinstance(v,datetime):
					# output[k] = v.isoformat()
					output[k] = v.strftime("%Y-%m-%d %H:%M:%S")
				else:
					output[k] = v

		return output

	def more_like_this(self,**kwargs):
		c = VWCollection(base_obj=self.__class__)
		return c.get_like_this(self.id).all(**kwargs)

class VWCollection():
	def __init__(self,**kwargs):

		self.results_per_page = 50
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
			idx = self.base_obj.idx
		else:
			idx = default_index

		self._search_params = []
		self._raw = {}
		self.idx = idx
		self.type = self.base_obj.__type__

	def _create_obj_list(self,es_rows):
		retlist = []
		for doc in es_rows:
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
		try:
			doc_list = self._es.mget(index=self.idx,doc_type=self.type,body={'ids':ids})
			docs = map( lambda d: self._create_obj(d), doc_list.get('docs') )
			return docs
		except:
			return []

	def get_like_this(self,doc_id):
		return self._create_obj_list( self._es.mlt(index=self.idx,doc_type=self.type,id=doc_id ) )

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
		
		params.update(kwargs)

		results = self._es.search( **params )
		rows = results.get('hits').get('hits')

		return self._create_obj_list( rows )
