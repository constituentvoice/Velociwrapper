from elasticsearch import Elasticsearch, NotFoundError,helpers
from datetime import date,datetime
from dateutil import parser
from uuid import uuid4
import json
import types
import copy
#from .config import dsn,default_index,bulk_chunk_size,logger
from . import config
from .config import logger
import elasticsearch
from .relationship import relationship
from .es_types import * # yeah yeah I know its "bad"
from traceback import format_exc

class ObjectDeletedError(Exception):
	pass

# Implements callbacks across objects
class VWCallback(object):
	callbacks = {}
	
	@classmethod
	def register_callback( cls, cbtype, callback ):
		if cls.__name__ not in cls.callbacks:
			cls.callbacks[cls.__name__] = {}

		if cbtype not in cls.callbacks[cls.__name__]:
			cls.callbacks[cls.__name__][cbtype] = []
		
		if not callable(callback):
			raise ValueError( 'parameter 2 to register_callback() must be callable' )

		cls.callbacks[cls.__name__][cbtype].append( callback )

	@classmethod
	def deregister_callback(cls, cbtype, callback_name):
		try:
			for cb in cls.callbacks[cls.__name__][cbtype]:
				if cb == callback_name or cb.__name__ == callback_name:
					cls.callbacks[cls.__name__][cbtype].remove(cb)
					break
		except KeyError:
			pass

	def execute_callbacks( self, cbtype, argument=None, **kwargs ):
		try:
			for cb in self.callbacks[self.__class__.__name__][cbtype]:
				argument = cb( self, argument, **kwargs )
		except KeyError:
			pass # no callbacks by this name. 

		return argument

class VWBase(VWCallback):
	# connects to ES
	_watch = False
	_needs_update = False
	id = ''
	
	def __init__(self,**kwargs):
		# the internal document
		self._document = {}

		# pickling off by default. Set by __getstate__ and __setstate__ when
		# the object is pickled/unpickled. Allows all values to be set
		self._pickling = False

		# relationships should not be executed when called from init (EVAR)
		self._no_ex = True
		
		if kwargs.get('_set_by_query'):
			self._new = False
			self._set_by_query = True
		else:
			self._new = True
			self.execute_callbacks('before_manual_create_object')

		self._needs_update = False
		self._watch = True
		
		# connect using defaults or override with kwargs
		self._es = elasticsearch.Elasticsearch( config.dsn )
		self._deleted = False

		if '__index__' not in dir(self):
			self.__index__ = config.default_index

		for k in dir(self):
			v = getattr(self,k)
			# skip functions and special variables
			if not isinstance(v,types.MethodType) and not k[0] == '_':

				# check if we were called with a variable. If so set
				try:
					v = kwargs[k]
				except KeyError:
					pass

				setattr(self,k,v)

		if 'id' not in kwargs:
			self.id = str(uuid4())

		# make sure we're ready for changes
		self._set_by_query = False
		self._no_ex = False
		if self._new:
			self.execute_callbacks('after_manual_create_object')
	
	# customizations for pickling
	def __getstate__(self):
		# mark as pickling
		self._pickling = True
	
		# copy the __dict__. Need copy so we don't
		# break things when flags are removed
		retval = {}
		
		for k,v in self.__dict__.iteritems():
			if k != '_es' and k != '_pickling':
				retval[k] = copy.deepcopy(v)

		self._pickling = False
		return retval

	def __setstate__(self,state):
		self._pickling = True

		for k,v in state.iteritems():
			setattr(self,k,v)

		#self._document = state.get('_document')
		#super(VWBase,self).__setattr__('_document',state.get('_document'))

		#setattr(self, '_document', state.get('_document') )

		# recreate the _es connection (doesn't reset for some reason)
		self._es = elasticsearch.Elasticsearch(config.dsn)
	
		self._pickling = False

	def __getattribute__(self,name):
		# ok this is much funky
		
		no_ex = False
		try:
			no_ex = super(VWBase,self).__getattribute__('_no_ex')
		except AttributeError:
			pass
		
		v = None
		doc = None
		try:
			doc = super(VWBase,self).__getattribute__('_document')
			if name in doc:
				v = doc.get(name)
		except AttributeError:
			pass
		
		if not v:
			v = super(VWBase,self).__getattribute__(name) 
			
			# instance attribute was becoming a refeence to the class attribute. Not what we wanted
			# make a copy
			if doc:
				if not isinstance(v,types.MethodType) and not name[0] == '_':
					v = copy.deepcopy(v)
					self._document[name] = v # setattribute doesn't seem to work :(
					return self._document[name]



		# we want to keep the relationships if set_by_query in the collection so we only execute with direct access
		# (we'll see, it might have an unintended side-effect)
		if isinstance(v,relationship) and not no_ex:
			return v.execute(self)

		elif isinstance(v,str) or isinstance(v,unicode):
			try:
				dt_value = datetime.strptime(v,'%Y-%m-%dT%H:%M:%S')
				return dt_value
			except ValueError:
				try:
					d_value = datetime.strptime(v,'%Y-%m-%d')
					return d_value.date()
				except:
					return v
			except:
				return v
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
			# TODO ... this stuff is probably going to have to be rethought
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
				if name not in dir(self) or name in ['_set_by_query','_deleted','_watch','_new','_no_ex','_pickling','_document'] or self._pickling:
					object.__setattr__(self,name,value)  # don't copy this stuff. Set it as is

			else:
				currvalue = create_es_type(currvalue) # create it as an es_type

				try:
					if value._metaclass__ == ESType:
						set_value_cls = False
					else:
						set_value_cls = True
				except AttributeError:
					set_value_cls = True
				
				if set_value_cls:

					type_enforcement = False
					try:
						type_enforcement = self.__strict_types__
					except AttributeError:
						try:
							type_enforcement = config.strict_types
						except AttributeError:
							type_enforcement = False

					try:
						if currvalue.__metaclass__ == ESType:
							cls = currvalue.__class__
							params = currvalue.es_args()

							# try to set the value as the same class.
							try:
								value = cls(value, **params)
							except:
								# value didn't set. Try to create it as its own es type
								test_value = create_es_type(value)

								# dates and times are special
								if isinstance(test_value,DateTime):
									if isinstance(currvalue, DateTime):
										value = DateTime(test_value.year, test_value.month, test_value.day, test_value.hour, test_value.minute, test_value.second, test_value.microsecond, test_value.tzinfo, **params)
									elif isinstance(currvalue, Date):
										value = Date(test_value.year, test_value.month, test_value.day,**params)
									else:
										value = test_value

								else:
									value = test_value
							

							if type_enforcement:
								try:
									if value.__class__ != currvalue.__class__:
										raise TypeError('strict type enforcement is enabled. ' + name + ' must be set with ' + currvalue.__class__.__name__)
								except:
									# errors where value isn't even a class will raise their own exception
									# caught here to avoid attribute errors from this block being passed along below
									raise

					except AttributeError:
						# currvalue couldn't be converted to an ESType
						# we just fall back to regular types.
						# if ES has an issue it will throw its own exception.
						pass
				
				#object.__setattr__(self,name,value)
				# just set the field on the document
				if isinstance(value,DateTime) or isinstance(value,datetime):
					self._document[name] = value.strftime('%Y-%m-%dT%H:%M:%S')
				elif isinstance(value,Date) or isinstance(value,date):
					self._document[name] = value.strftime('%Y-%m-%d')
				else:
					self._document[name] = value

				if self._watch:
					object.__setattr__(self,'_needs_update',True)
					object.__setattr__(self,'_watch',False)

	def commit(self):
		# save in the db


		if self._deleted and self.id:
			self.execute_callbacks('on_delete')
			self._es.delete(id=self.id,index=self.__index__,doc_type=self.__type__)
		else:
			self.execute_callbacks('before_commit')
			idx = self.__index__
			doc_type = self.__type__

			doc = self._document
			#for k,v in self.__dict__.iteritems():
			#	if k[0] == '_' or k == 'id':
			#		continue

			#	if isinstance(v,relationship): # cascade commits? # probably not
			#		continue

			#	doc[k] = v

			res = self._es.index(index=idx,doc_type=doc_type,id=self.id,body=doc)
			self._watch = True
			self.execute_callbacks('after_commit')


	def sync(self):
		if self.id:
			try:
				self.execute_callbacks('before_sync')
				res = self._es.get(id=self.id,index=self.__index__)
				#for k,v in res.get('_source').iteritems():
				#	if self.__class__.__dict__.get(k) and ( isinstance( self.__class__.__dict__.get(k), date ) or isinstance( self.__class__.__dict__.get(k), datetime ) ):
				#		try:
				#			v = parser.parse(v)
				#		except:
				#			pass

				#	setattr(self,k,v)
				self._document = res.get('_source')

				self._new = False
				self.execute_callbacks('after_sync')

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
		# copy so we dont overwrite the original document
		return copy.deepcopy(self._create_source_document(datetime_format='%Y-%m-%d %H:%M:%S') ) # to_dict should use true ISO format without the T

	def _create_source_document(self, **kwargs):
		output = self._document
		return output

	def more_like_this(self,**kwargs):
		c = VWCollection(base_obj=self.__class__)
		return c.get_like_this(self.id).all(**kwargs)
