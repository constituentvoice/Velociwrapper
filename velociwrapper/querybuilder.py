class QueryBody(object):
	def __init__(self):
		self._filters = []
		self._queries = []
		self._condition_chain = []
		self._nest = []
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
		
		top_list = getattr(self, self._last_part

		if 'bool' in newpart:

				



		


	

	
	
