def query(params):
	return { "query": params }

def match(field,value,**kwargs):
	output = { "match": { field: { "query": value } } }
	for k,v in kwargs.iteritems():
		if k not in ['boost', 'rewrite', 'operator', 'minimum_should_match', 'prefix_length', 'max_expansions', 'constant_score_rewrite', 'fuzzy_rewrite', 'zero_terms_query', 'cutoff_frequency','stopwords','type','analyzer','lenient']:
			raise KeyError( "%s is not a valid keyword argument for match()" % k )

		output['match'][field][k] = v
	return output

def match_phrase(field, value, **kwargs ):
	kwargs['type'] = "phrase"
	return match( field, value, **kwargs )

def match_phrase_prefix(field, value, **kwargs ):
	kwargs['type'] = "phrase_prefix"
	return match( field, value, **kwargs )

def multi_match( query, fields, **kwargs):
	output = { "multi_match": { "query":  query, "fields": fields } }

	for k,v in kwargs.iteritems():
		if k not in ['boost', 'rewrite', 'operator', 'minimum_should_match', 'prefix_length', 'max_expansions', 'fuzziness', 'zero_terms_query', 'cutoff_frequency','stopwords','type','analyzer','tie_breaker']:
			raise KeyError( "%s is not a valid keyword argument for multi_match()" % k )

		output["multi_match"][k] = v

	return output

def bool(*args, **kwargs):
	output = {"bool": {} }
	
	if len(args) == 1 and isinstance(args[0], dict):
		output['bool'] = args[0]
	else:
		for arg in args:
			if not isinstance(arg, dict):
				raise TypeError("Arguments to bool() must be type dict")
		
			if "must" in arg:
				output['bool']['must'] = arg['must']

			if "must_not" in arg:
				output['bool']['must_not'] = arg['must_not']

			if 'should' in arg:
				output['bool']['should'] = arg['should']

	for k,v in kwargs.iteritems():
		output['bool'][k] = v

	return output

def bool_param( bool_type, params, value=None, **kwargs )
	if isinstance(params, str) or isinstance(params,unicode) or value:
		# assume field / value
		return { bool_type: { "term": { params: value } } }
	else:
		return { bool_type: params }

def must( params, value=None, **kwargs ):
	return bool_param("must", params, value, **kwargs )

def must_not( params, value=None, **kwargs):
	return bool_param("must_not", params, value, **kwargs )

def should( params, value=None, **kwargs ):
	return bool_params("should", params, value, **kwargs )

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

	for k,v in kwargs.iteritems():
		output['boosting'][k] = v

	return output

def common( field, value, **kwargs ):
	output = {"common": { field: { "query": value } } }

	for k,v in kwargs.iteritems():
		if k not in ["cutoff_frequency", "minimum_should_match", 
