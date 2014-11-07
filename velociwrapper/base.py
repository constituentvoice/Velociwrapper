from elasticsearch import Elasticsearch, NotFoundError,helpers
from datetime import date,datetime
from dateutil import parser
from uuid import uuid4
import json
import types
import copy
from .config import dsn,default_index,bulk_chunk_size,logger,es
from .relationship import relationship
from .es_types import * # yeah yeah I know its "bad"

class ObjectDeletedError(Exception):
	pass

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
		self._es = es
		self._deleted = False

		if '__index__' not in dir(self):
			self.__index__ = default_index

		for k in dir(self):
			v = getattr(self,k)
			# skip functions and special variables
			if not isinstance(v,types.MethodType) and not k[0] == '_':

				# check if we were called with a variable. If so set
				if kwargs.get(k):
					new_v = create_es_type(kwargs.get(k))
					setattr(self,k,new_v)


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
			currvalue = create_es_type(currvalue) # create it as an es_type
			try:
				if currvalue.__metaclass__ == ESType:
					cls = currvalue.__metaclass__
					set_value_cls = False

					try:
						if value.__metaclass__ != EStype:
							set_value_cls = True
					except AttributeError:
						set_value_cls = True

					if set_value_cls:
						value = cls(value)
			except AttributeError:
				pass

								
			if name[0] == '_':
				# special rules for names with underscores.
				# seting the _ values will not trigger an update. 
				if name not in dir(self) or name in ['_set_by_query','_deleted','_watch','_new','_no_ex']:
					object.__setattr__(self,name,value)  # don't copy this stuff. Set it as is

			else:
				# find es type from key if it exists
				
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
