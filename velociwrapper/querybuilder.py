class QueryBody(object):
	def __init__(self):
		self._filters = self._queries = { 'must':[], 'should':[], 'should_not' }
		self._bool = 'must'
		self._last_part = 'query'


	def chain(self,newpart,**kwargs):
		if kwargs.get('type') in ['filter','query']:
			self._last_part = kwargs.get('type')
		elif isinstance(newpart,dict):
			if 'filter' in newpart:
				self._last_part = 'filter'
				newpart = newpart.get('filter')
			elif 'query' in newpart:
				self._last_part = 'query'
				newpart = newpart.get('query')
		
		if kwargs.get('condition'):
			self._bool = kwargs.get('condition')

		# newpart is already a bool. We'll split it up
		if 'bool' in newpart:
			newpart = newpart.get('bool')
		
		top_list = getattr(self, self._last_part)
		
		# find top level bools
		_found_part = False
		for t in ['must','should','should_not']:
			if t in newpart:
				_found_part = True
				if isinstance(newpart, list):
					top_list[t].extend(newpart)
				else:
					top_list[t].append(newpart)
			
		if not _found_part:
			top_list[self._bool].append(newpart)

		return self
	
	def build(self):
		q = {}
		is_filtered = False
		for t in ['must','should','should_not']:
			if len(self._filters[t]) > 0:
				is_filtered = True
				break

		if is_filtered:
			q = {'filtered': {'filter':{} } }
			



	


				



		


	

	
	
