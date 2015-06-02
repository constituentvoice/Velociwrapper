class QueryBody(object):
	def __init__(self):
		self._filter = { 'must':[], 'should':[], 'should_not':[], 'and':[], 'not':[], 'or':[] }
		self._query = { 'must':[], 'should':[], 'should_not':[] }
		self._bool = 'must'
		self._last_part = '_query'
		self._explicit = 'and'

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
		
		# newpart is already a bool. We'll split it up
		if 'bool' in newpart:
			newpart = newpart.get('bool')

		if kwargs.get('with_explicit'):
			self._explicit = kwargs.get('with_explicit')

		if kwargs.get('condition'):
			if kwargs.get('condition') in ['and','or','not']:
				self._explicit = kwargs.get('condition')
			else:
				self._bool = kwargs.get('condition')

		top_list = getattr(self, self._last_part)
		
		# find top level bools
		_found_part = False
		for t in ['must','should','should_not']:
			if t in newpart:
				thispart = newpart[t]
				_found_part = True
				if isinstance(newpart, list):
					top_list[t].extend(thispart)
				else:
					top_list[t].append(thispart)

		# explicit conditions for filters
		for t in ['and','or','not']:
			if t in newpart:
				if self._last_part == '_query': # make sure are a filter
					raise ValueError( 'Type of new chained QDSL must be a filter to use explicit conditions' )

				thispart = newpart[t]
				_found_part = True
			
		if not _found_part:
			top_list[self._bool].append(newpart)

		return self

	def is_filtered(self):
		for t in ['must','should','should_not','and','or','not']:
			if len(self._filter[t]) > 0:
				return True

	def is_query(self):
		for t in ['must','should','should_not']:
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
		
		for t in ['must','should','should_not']:
			if len(_filter[t]) > 0:
				is_filtered = True
				f_type_count += 1
				f_type = t
				if len(_filter[t]) > 1:
					filter_is_multi_condition = True
			else:
				del _filter[t]

			if len(_query[t]) > 0:
				is_query = True
				q_type_count += 1
				q_type = t
				if len(_query[t]) > 1:
					query_is_multi_condition = True
				else:
					_query[t] = _query[t][0] # if only one remove the list
			else:
				del _query[t]

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
			else:
				_output_filter = _filter[f_type]
			_output_filter = { 'filter': _output_filter }

			_output_query['filter'] = _output_filter
			_output_query = { 'filtered': _output_query }

		return _output_query
