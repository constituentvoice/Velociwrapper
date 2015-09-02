def query(params):
	return { "query": params }

def filter_(params):
	return { "filter": params }

def match(field,value,**kwargs):
	output = { "match": { field: { "query": value } } }
	output['match'].update(kwargs)
	
	return output

def match_phrase(field, value, **kwargs ):
	kwargs['type'] = "phrase"
	return match( field, value, **kwargs )

def match_phrase_prefix(field, value, **kwargs ):
	kwargs['type'] = "phrase_prefix"
	return match( field, value, **kwargs )

def multi_match( query, fields, **kwargs):
	output = { "multi_match": { "query":  query, "fields": fields } }
	output["multi_match"].update(kwargs)
	return output

def bool(*args, **kwargs):
	output = kwargs.get('__vw_set_current', {"bool": {} })
	try:
		del kwargs['__vw_set_current']
	except KeyError:
		pass
	
	if len(args) == 1 and isinstance(args[0], dict):
		output['bool'] = args[0]
	else:
		for arg in args:
			if not isinstance(arg, dict):
				raise TypeError("Arguments to bool() must be type dict")

			_bool_type = None
			if "must" in arg:
				_bool_type = 'must'

			if "must_not" in arg:
				_bool_type = 'must_not'

			if 'should' in arg:
				_bool_type = 'should'

			if _bool_type in output['bool']:
				if not isinstance(output['bool'][_bool_type], list):
					output['bool'][_bool_type] = [ output['bool'][_bool_type] ]

				output['bool'][_bool_type].append( arg[_bool_type] )
			else:
				output['bool'][_bool_type] = arg[_bool_type]

	output['bool'].update(kwargs)
	return output

def term(field,value,**kwargs):
	output = { 'term': { field: { 'value': value }  } }
	output['term'][field].update(kwargs)
	return output

def terms(field, value,**kwargs):
	if not isinstance(value,list):
		raise TypeError('terms() requires a list as value.')
	
	output = {'terms': {field: value}}
	output['terms'].update(kwargs)
	return output


def _term_param( _term_type, params, value=None, **kwargs ):
	if isinstance(params, str) or isinstance(params,unicode) or value:
		# assume field / value
		return { _term_type: term(params,value) }
	else:
		return { _term_type: params }

def must( params, value=None, **kwargs ):
	return _term_param("must", params, value, **kwargs )

def must_not( params, value=None, **kwargs):
	return _term_param("must_not", params, value, **kwargs )

def should( params, value=None, **kwargs ):
	return _term_param("should", params, value, **kwargs )

def boosting(*args, **kwargs):
	output = {"boosting": {} }

	if len(args) == 1 and isinstance(args[0], dict):
		output['boosting'] = args[0]
	else:
		for arg in args:
			if not isinstance(arg, dict):
				raise TypeError("Arguments to boosting() must be type dict")

			if "positive" in arg:
				output['boosting']['positive'] = arg['positive']

			if "negative" in arg:
				output['boosting']['negative'] = arg['negative']
	
	output['boosting'].update(kwargs)
	return output

def positive(params,value):
	return _term_param('positive', params, value )

def negative(params,value):
	return _term_param('negative', params, value )

def common( field, value, **kwargs ):
	output = {"common": { field: { "query": value } } }
	output['common'][field].update(kwargs)
	return output

def constant_score( *args, **kwargs ):
	output = {'constant_score': {} }
	if len(args) == 1 and isinstance(args[0], dict ):
		output['constant_score'] = args[0]
	
	else:
		for arg in args:
			if not isinstance(arg, dict):
				raise TypeError('Arguments to constant_score() must be type dict')

			if 'filter' in arg:
				output['constant_score']['filter'] = arg['filter']
			
			if 'query' in arg:
				output['constant_score']['query'] = arg['query']
	
	output['constant_score'].update(kwargs)
	return output

def filtered( *args, **kwargs ):
	output = { 'filtered': {} }
	if len(args) == 1 and isinstance(args[0], dict):
		output['filtered'] = args[0]
	else:
		for arg in args:
			if not isinstance(arg, dict):
				raise TypeError('Arguments to filtered() must be type dict')

			if 'filter' in arg:
				output['filtered']['filter'] = arg['filter']

			if 'query' in arg:
				output['filtered']['query'] = arg['query']
		
	output['filtered'].update(kwargs)
	return output

# mostly for completeness. Not much magic going on here 
def function_score( *args, **kwargs ):
	output = { 'function_score': {} }
	
	if len(args) == 1 and isinstance(args[0], dict):
		output['function_score'] = args[0]
	else:

		for arg in args:
			if not isinstance(arg, dict):
				raise TypeError('arguments to filtered must be type dict')

			if 'filter' in arg:
				output['function_score']['filter'] = arg['filter']

			if 'query' in arg:
				output['function_score']['query'] = arg['query']

			if 'FUNCTION' in arg:
				output['function_score']['FUNCTION'] = arg['FUNCTION']

			if 'functions' in arg:
				output['function_score']['functions'] = arg['functions']

	output['function_score'].update(kwargs)
	return output

def fuzzy(field, value):
	return { 'fuzzy': { field: value } }

def ids(*values,**kwargs):
	if isinstance(values[0],list):
		values = values[0]
	elif len(values) == 1 and not isinstance(values,list):
		values = [values]

	output = { 'ids': { 'values': values } }
	if kwargs.get('type'):
		output['ids']['type'] = kwargs.get('type')
	
	return output

def query_term(field, value):
	return _term_param('query',field,value)

def indices(*args, **kwargs):
	output = { 'indices': {} }
	if len(args) == 1 and isinstance(args[0], dict):
		output['indices'] = args[0]

	else:
		if isinstance(args[0], list):
			# treat as indices
			output['indices']['indices'] = args[0]

		for arg in args[1:]:
			if not isinstance(arg, dict):
				raise TypeError('Subsequent arguments to indices() must be dict')

			if 'query' in arg:
				output['indices']['query'] = arg['query']

			if 'no_match_query' in arg:
				output['indices']['no_match_query'] = arg['no_match_query']

		output['indices'].update( kwargs )
	
	return output

def match_all(*args,**kwargs):
	output = { "match_all": {} }
	output['match_all'].update(kwargs)
	return output

def more_like_this(*args, **kwargs):
	output = { "more_like_this": {} }
	if len(args) == 1 and isinstance( args[0], dict):
		output['more_like_this'] = args[0]
	
	output['more_like_this'].update(kwargs)

	return output

def nested(*args, **kwargs):
	output = { "nested": {} }
	
	if len(args) == 1 and isinstance(args[0], dict):
		output['nested'] = args[0]

	output['nested'].update(kwargs)

	return output

def prefix(field,value=None,**kwargs):
	output = {"prefix": {} }
	if isinstance(field,dict):
		output['prefix'] = field
	else:
		output['prefix'] = {field: {'value':value}}

		output['prefix'][field].update(kwargs)
	
	return output

def query_string(string, **kwargs):
	output = { "query_string": { "query": string } }
	output['query_string'].update(kwargs)

	return output

def simple_query_string(string, **kwargs):
	return query_string(string, **kwargs)

def range( field, **kwargs ):
	output = { "range": { } }

	if isinstance(field, dict):
		output['range'] = field
	else:
		output['range'][field] = kwargs
	
	return output

def regexp( field, value=None, **kwargs ):
	output = { 'regexp': {} }
	if isinstance( field, dict):
		output['regexp'] = field
	else:
		output = {'regexp': { field: { 'value': value } } }
		output['regexp'][field].update(kwargs)

	return output

def span_term( field, value, **kwargs ):
	output = { "span_term": { field: { 'value': value } } }
	output['span_term'][field].update(kwargs)
	return output

def span_first( arg=None, **kwargs ):
	output = { 'span_first': {} }

	if arg and not isinstance(arg, dict):

		raise TypeError('Argument to span_first() must be dict (ether a term or another span query, or a match)')
	
	found_arg = False
	if arg:
		if 'match' in arg:
			output['span_first'] = arg
			found_arg = True
		else:
			for qtype in ['span_term', 'span_multi', 'span_near','span_not','span_or','span_term']:
				if qtype in arg:
					found_arg = True
					output['span_first']['match'] = arg
					break
	
	if 'end' in kwargs:
		output['span_first']['end'] = kwargs.get('end')
		del kwargs['end']

	if not found_arg:
		output['span_first']['match'] = {}
		for k,v in kwargs:
			output['span_first']['match'] = span_term(k,v)
			break

	return output

def span_multi(query=None, **kwargs):
	output = {"span_multi":{'match': {}} }

	if query:
		output['span_multi']['match'] = query

	if 'match' in kwargs:
		output['span_multi']['match'] = kwargs.get('match')

	return output

def span_near(*clauses,**kwargs):
	if len(clauses) == 1 and isinstance(clauses[0], list):
		clauses = clauses[0]

	output = { "span_near": {'clauses': clauses } }
	
	output['span_near'].update(kwargs)

	return output

def span_not( **kwargs ):
	return { "span_not": { "include": kwargs.get('include'), "exclude": kwargs.get('exclude') } }

def span_or( *clauses, **kwargs ):
	if len(clauses) == 1 and isinstance(clauses[0], list):
		clauses = clauses[0]

	output = { "span_or": {'clauses': clauses } }
	
	output['span_or'].update(kwargs)

	return output

def wildcard(field, value, **kwargs):
	output = { "wildcard": { field: { 'value': value } } }
	output['wildcard'][field].update( kwargs )
	return output

def and_(*filters, **kwargs):
	if len(filters) == 1 and isinstance(filters[0],list ):
		filters = filters[0]
	elif len(filters) == 1:
		filters = [filters]

	output = { "and": { 'filters': filters }}
	output['and'].update(kwargs)
	return output

def not_(filter_, **kwargs):
	return {"not": filter_, '_cache': kwargs.get('_cache',False) }

def or_(*filters, **kwargs):
	if len(filters) == 1 and isinstance(filters[0],list ):
		filters = filters[0]
	elif len(filters) == 1:
		filters = [filters]

	return {"or": { 'filters': filters, '_cache': kwargs.get('_cache',False) } }

def exists(field,**kwargs):
	return { "exists": { "field": field } }

def geo_bounding_box(field,**kwargs):
	return { "geo_bounding_box": { field: kwargs } }

def geo_distance( field, point, distance,**kwargs ):
	output = { "geo_distance": { "distance": distance, field: point } }
	output['geo_distance'].update(kwargs)
	return output

def geo_range( field,point,from_dist,to_dist,**kwargs):
	output = {"geo_distance_range": {field: point, "from": from_dist, "to":to_dist } }
	output['geo_distance_range'].update(kwargs)
	return output

def geo_polygon( field, points, **kwargs ):
	output = {"geo_polygon": { field: points } }
	output['geo_polygon'].update(kwargs)
	return output

def geo_shape(field,**kwargs):
	output = { "geo_shape": { field: {} } }

	if kwargs.get('shape'):
		output['geo_shape'][field]['shape'] = kwargs.get('shape')

	elif kwargs.get('indexed_shape'):
		output['geo_shape'][field]['indexed_shape'] = kwargs.get('indexed_shape')

	return output

def geohash_cell(field,lat,lon,**kwargs):
	output = { "geohash_cell": { field: { "lat": lat, "lon": lon } } }
	output['geohash_cell'].update(kwargs)
	return output

def has_child(_type,**kwargs):
	output = { 'has_child': { 'type': _type } }
	output['has_child'].update(kwargs)
	return output
	

def has_parent(_type,**kwargs):
	output = { 'has_parent': { 'type': _type } }
	output['has_parent'].update(kwargs)
	return output

def missing( field, **kwargs ):
	return {"missing": {"field": field } }

def script(script, **kwargs ):
	output = { "script": { "script": script } }
	output['script']['script'].update(kwargs)
	return output

def type_(_type):
	return {"type": { "value": _type } }

