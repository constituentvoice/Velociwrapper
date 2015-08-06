import copy
from . import qdsl

class QueryBody(object):
	def __init__(self):
		self._filter = { 'must':[], 'should':[], 'must_not':[], 'and':[], 'not':[], 'or':[] }
		self._query = { 'must':[], 'should':[], 'must_not':[] }
		self._bool = 'must'
		self._last_part = '_query'
		self._explicit = None

	def chain( self, newpart, **kwargs ):

		condition_map = {'must':'and', 'should':'or', 'must_not': 'not' }
		condition_rev_map = {'and':'must', 'or':'should', 'not': 'must_not' }
		# figure out if we're a filter or a query being chained

		# explicitly stated
		if kwargs.get('type'):
			self._last_part = '_' + kwargs.get('type')
		elif isinstance(newpart, dict):
			_chained = False
			# comes in from dict, recursively chain each part
			if 'filter' in newpart:
				kwargs['type'] = 'filter'
				self.chain(newpart['filter'], **kwargs)
				_chained = True

			if 'query' in newpart:
				kwargs['type'] = 'query'
				self.chain(newpart['query'], **kwargs)
				_chained = True

			# we should have completed the chain at this point so just return
			if _chained:
				return self
		# else uses _last_par and assumes query if not specified
		
		# Explicit conditions only apply to filters
		if self._last_part == '_filter':
			# figure out if we have a top level condition (and, or, not)
			# check if its explicitly stated
			if kwargs.get('with_explicit'):
				self._explicit = kwargs.get('with_explicit')
			# Check if the condition parameter is really a top level
			elif kwargs.get('condition') in ['and','or','not']:
				self._explicit = kwargs.get('condition')

				# reverse the regular condition, this allows for one set of code down below (even if it gets converted back)
				kwargs['condition'] = condition_rev_map[kwargs['condition']]

			# check the part for explicit conditions
			elif isinstance(newpart, dict):
				_chained = False
				for t in ['and','or','not']:
					if t in newpart:
						kwargs['with_explicit'] = t
						self.chain( newpart, **kwargs )
						_chained = True

				# chained above should complete
				if _chained:
					return self
			# else there are no new explicit conditions. If _explicit is set we
			# will use the last value

			# go ahead and parse the newpart. This way when adding to the explicit conditions
			# vs non-explicit we won't have to parse it twice
			# existing conditions in the dictionary will just get parsed recursively
			_condition = 'must'
			if kwargs.get('condition') in ['must','should','must_not']:
				_condition = kwargs.get('condition')
			elif isinstance(newpart, dict):
				_chained = False
				for lt in ['must','should','must_not']:
					kwargs['condition'] = lt
					self.chain( newpart[lt], **kwargs)
					_chained = True
				if _chained:
					return self
			# else treat the condition as 'must'

			if self._explicit:
				# check to see if we need to move existing bools inside an explicit condition
				for btype, ttype in condition_map:
					if self._filter.get(btype):
						self._filter[ttype].extend( self._filter[btype] )

						del self._filter[btype]
				
				# we definitely have explicit conditions. Turn the condition into the explicit
				self._explicit = _condition = condition_map[_condition]
				self[_condition].append(newpart)
			else:
				# chain the newpart to the toplevel bool
				if _condition not in self._filter:
					self._filter[_condition] = []

				self._filter[_condition].append( newpart )
		# queries are much simpler
		else:
			_condition = 'must'
			if kwargs.get('condition') in ['must','should','must_not']:
				_condition = kwargs.get('condition')
			elif isinstance(newpart, dict):
				_chained = False
				for lt in ['must','should','must_not']:
					kwargs['condition'] = lt
					self.chain( newpart[lt], **kwargs)
					_chained = True

				if _chained:
					return self
			# else assume must

			if _condition not in self._query:
				self._query[_condition] = []

			self._query[_condition].append( newpart )

		return self

	"""
	def chain(self,newpart,**kwargs):
		if kwargs.get('type') in ['filter','query']:
			self._last_part = '_' + kwargs.get('type')

		elif isinstance(newpart,dict):
			if 'filter' in newpart:
				self._last_part = '_filter'
				newpart = newpart.get('filter')
			elif 'query' in newpart:
				self._last_part = '_query'
				newpart = newpart.get('query')

		# check for wrapping bools
		# chain them recursively
		_found_tl_condition = False
		if self._last_part == '_filter': # only apply to filters (queries can't have and,or,not
			for t in ['and','or','not']:
				if t in newpart:
					_found_tl_condition = True
					self._explicit = t  # sets the last upper bool for this query
					self.chain( newpart[t], condition=t )

		# the with_explicit parameter tells us to place the additional part under the explicit and,or,not
		if kwargs.get('with_explicit'):
			self._explicit = kwargs.get('with_explicit')
	
		condition = False
		if not _found_tl_condition:
			
			# check for a condition paramter. 
			tl_condition = None
			if kwargs.get('condition'):
				if kwargs.get('condition') in ['and','or','not']:
					self._explicit = kwargs.get('condition')
					tl_condition = self._explicit
					condition = self._explicit

					# if we have and, or, not conditions. The bool conditions need to be contained under them. We contain them in the last called condition
					bool_args = []
					for t in ['must','should','must_not']:
						cond_method = getattr( qdsl, t )
						if len(self._filter[t]) > 1:
							bool_args.append( cond_method( qdsl.terms( self._filter[t] ) ) )
							self._filter[t] = []
						elif( len(self._filter[t] ) > 0 ):
							bool_args.append( cond_method( self._filter[t] ) )
							self._filter[t] = []
					
					self._filter[condition].append( qdsl.bool( *bool_args ) )

				else:
					self._bool = kwargs.get('condition')
					condition = kwargs.get('condition')

				# Filters come first
				if self._last_part == '_filter':
					# if we have and, or, not bools then convert the bool query bools
					if self._explicit and not tl_condition:
						ex_match = {'must': 'and', 'should': 'or', 'must_not': 'not' }
						new_condition = ex_match.get(condition)
						if new_condition:
							condition = new_condition
						else:
							raise ValueError( "Invalid condition '%s' provided to chain()" % condition )

					if isinstance( newpart,list ):
						self._filter[condition].append( qdsl.terms( newpart ) )
					else:
						# should be a dictionary!
						self._filter[condition].append( newpart )
				elif self._last_part == '_query':
					# now filters
					if isinstance( newpart,list ):
						self._query[condition].append( qdsl.terms( newpart ) )
					else:
						self._query[condition].append( newpart )
				else:
					raise ValueError( 'Arguments to chain() must contain a query or filter' )

			else:
				# no condition specified. Try to figure out if the elements are contained inside
				# newpart is already a bool. We'll split it up
				if 'bool' in newpart:
					newpart = newpart.get('bool')
				
				_found_ll = False
				for t in ['must','should','must_not']:
					if t in newpart:
						# chain each part separately
						_found_ll = True
						self.chain( newpart[t], condition=t )


				# Regular condition
				if not _found_ll:
					self.chain( newpart, condition='must')

		return self
	"""

	def is_filtered(self):
		for t in ['must','should','must_not','and','or','not']:
			if len(self._filter[t]) > 0:
				return True

	def is_query(self):
		for t in ['must','should','must_not']:
			if len(self._query[t] ) > 0:
				return True

	def build(self):
		q = {}
		is_filtered = False
		is_query = False
		filter_is_multi_condition = False
		query_is_multi_condition = False

		f_type_count = 0
		q_type_count = 0

		# gets set to the last detected type
		# used if type_counts end up being one 
		# to quickly access the 
		q_type = None
		f_type = None

		# copy the filters and queries so the chain is still intact. Need so collections act the same as before
		_query = copy.deepcopy( self._query )
		_filter = copy.deepcopy(self._filter )
		
		for t in ['and', 'or','not', 'must','should','must_not']:
			if len(_filter[t]) > 0:
				is_filtered = True
				f_type_count += 1
				f_type = t
				if len(_filter[t]) > 1:
					filter_is_multi_condition = True
				else:
					_filter[t] = _filter[t][0] # if only one remove the list
			else:
				try:
					del _filter[t]
				except KeyError:
					pass

			if len(_query[t]) > 0:
				is_query = True
				q_type_count += 1
				q_type = t
				if len(_query[t]) > 1:
					query_is_multi_condition = True
				else:
					_query[t] = _query[t][0] # if only one remove the list
			else:
				try:
					del _query[t]
				except KeyError:
					pass

		if f_type_count > 1:
			filter_is_multi_condition = True

		if q_type_count > 1:
			query_is_multi_condition = True

		_output_query = {}
		if is_query:
			if query_is_multi_condition:
				_output_query = { 'bool': _query }
			else:
				_output_query = _query[q_type]

			_output_query = { 'query': _output_query }
		else:
			_output_query = { 'query': { 'match_all': {} } }

		if is_filtered:
			_output_filter = {}
			if filter_is_multi_condition:
				_output_filter = { 'bool': _filter }
			elif len(_filter[f_type]) == 1:
				_output_filter = _filter[f_type][0]
			else:
				_output_filter = _filter[f_type]

			#_output_filter = { 'filter': _output_filter }

			_output_query['filter'] = _output_filter
			_output_query = {'query': { 'filtered': _output_query } }

		return _output_query
