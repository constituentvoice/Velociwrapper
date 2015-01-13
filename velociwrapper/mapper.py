from . import config
from elasticsearch import Elasticsearch, client, helpers
from .config import logger
from .relationship import relationship
from .es_types import *
from .base import VWBase
import json

# tools for creating or reindexing elasticsearch mapping
class Mapper(object):
	def __init__(self):
		self._es = Elasticsearch(config.dsn)
		self._esc = client.IndicesClient(self._es)

	# Retrieves the mapping as defined by the server
	def get_server_mapping(self,**kwargs):
		indexes = []
		if isinstance(kwargs.get('index'),list):
			indexes = kwargs.get('index')
		elif kwargs.get('index'):
			indexes.append(kwargs.get('index'))

		# if the model arguent is a VWBase object
		if isinstance(kwargs.get('index'), VWBase):
			try:
				indexes.append(kwargs.get('index').__index__)
			except AttributeError:
				pass

		if not indexes:
			indexes.append(config.default_index)

		return self._esc.get_mapping(index=indexes)

	# Retrieves what the map should be according to the defined models
	def get_index_map(self, **kwargs ):
		# recursively find all the subclasses of base

		# options
		"""
		* index = "string" 
			only map the index defined by "string"

		* index = ['index1','index2' ...]
			map the indexes defined by entries in list
		
		"""

		subclasses = []
		self.get_subclasses(VWBase,subclasses)

		indexes = {}

		index_list = []
		if kwargs.get('index'):
			if isinstance(kwargs.get('index'), str):
				index_list.append( kwargs.get('index'))

			elif isinstance(kwargs.get('index'), list):
				index_list.extend( kwargs.get('index') )
			else:
				raise TypeError('"index" argument must be a string or list')


		for sc in subclasses:

			try:
				idx = sc.__index__
			except AttributeError:
				idx = config.default_index

			if len(index_list) > 0 and idx not in index_list:
				continue

			if idx not in indexes:
				indexes[idx] = {"mappings": {} }

			try:
				# create the basic body
				sc_body = { sc.__type__: { "properties": {} } }
			except AttributeError:
				# fails when no __type__ is found. Likely a subclass
				# to add other features. We will skip mapping
				continue
				

			for k,v in sc.__dict__.iteritems():
				try:
					if v.__metaclass__ == ESType:
						sc_body[sc.__type__]['properties'][k] = v.prop_dict()
				except AttributeError:
					pass

			indexes[idx]['mappings'].update(sc_body)
			
		return indexes

	def create_indicies(self, **kwargs):
		indexes = self.get_index_map(**kwargs)

		for k,v in indexes.iteritems():
			self._esc.create( index=k, body=v )
	
	def get_index_for_alias(self, alias):
		aliasd = self._esc.get_aliases(index=alias)
		index = ''
		for k,v in aliasd.iteritems():
			index = k
			break

		if index == alias:
			return None

		return index

			

	def reindex(self, idx, newindex, **kwargs):
		# are we an alias or an actual index?
		index = idx;
		alias = None
		alias_exists = False

		if self._esc.exists_alias(idx):
			alias = idx
			index = self.get_index_for_alias(idx)
			alias_exists = True

		if kwargs.get('alias_name'):
			alias = kwargs.get('alias_name')

		# does the new index exist?
		if not self._esc.exists( newindex ):
			# if new doesn't exist then create the mapping
			# as a copy of the old one. The idea being that the mapping 
			# was changed
			index_mapping = self.get_index_map(index=idx) # using "idx" intentionally because models will be defined as alias
			self._esc.create( index=newindex, body=index_mapping)
			
		# map our documents
		helpers.reindex(self._es, index, newindex, **kwargs)

		if kwargs.get('remap_alias'):
			if alias_exists:
				self._esc.delete_alias(alias)

			self._esc.put_alias(name=alias,index=newindex)


	def get_subclasses(self,cls,subs):
		this_subs = cls.__subclasses__()
		if len(this_subs) == 0:
			subs.append(cls)
		else:
			for sc in this_subs:
				self.get_subclasses(sc,subs)
	
	def describe(self,cls):
		body = {}
		for k,v in cls.__dict__.iteritems():
			try:
				if v.__metaclass__ == ESType:
					body[k] = v.prop_dict()
			except AttributeError:
				pass

			if not body.get(k):
				body[k] = { "type": type(v).__name__ }

		return body
