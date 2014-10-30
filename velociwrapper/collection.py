from elasticsearch import Elasticsearch, NotFoundError,helpers
from datetime import date,datetime
from dateutil import parser
from uuid import uuid4
import json
import types
import copy
import logging
from .config import dsn,default_index,bulk_chunk_size,logger

# Raised when no results are found for one()
class NoResultsFound(Exception):
	pass


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
