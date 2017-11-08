from __future__ import absolute_import
from six import iteritems, string_types

__all__ = ['query', 'filter_', 'match', 'match_phrase', 'match_phrase_prefix', 'multi_match', 'bool_', 'term', 'terms',
           'must', 'must_not', 'should', 'boosting', 'positive', 'negative', 'common', 'constant_score',
           'function_score', 'fuzzy', 'ids', 'query_term', 'indices', 'match_all', 'more_like_this', 'nested', 'prefix',
           'query_string', 'simple_query_string', 'range_', 'regexp', 'span_term', 'span_first', 'span_multi',
           'span_near', 'span_not', 'span_or', 'wildcard', 'exists', 'geo_bounding_box', 'geo_distance', 'geo_range',
           'geo_polygon', 'geo_shape', 'geohash_cell', 'has_child', 'has_parent', 'missing', 'script', 'type_',
           'highlight']


def query(params):
    return {"query": params}


def filter_(params):
    return {"filter": params}


def match(field, value=None, **kwargs):
    if isinstance(field, dict):
        output = {'match': field}
    else:
        output = {"match": {field: {"query": value}}}

    output['match'].update(kwargs)

    return output


def match_phrase(field, value, **kwargs):
    kwargs['type'] = "phrase"
    return match(field, value, **kwargs)


def match_phrase_prefix(field, value, **kwargs):
    kwargs['type'] = "phrase_prefix"
    return match(field, value, **kwargs)


def multi_match(_query, fields, **kwargs):
    output = {"multi_match": {"query": _query, "fields": fields}}
    output["multi_match"].update(kwargs)
    return output


def bool_(*args, **kwargs):
    output = kwargs.get('__vw_set_current', {"bool": {}})
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
                    output['bool'][_bool_type] = [output['bool'][_bool_type]]

                output['bool'][_bool_type].append(arg[_bool_type])
            else:
                output['bool'][_bool_type] = arg[_bool_type]

    output['bool'].update(kwargs)
    return output


def term(field, value, **kwargs):
    output = {'term': {field: {'value': value}}}
    output['term'][field].update(kwargs)
    return output


def terms(field, value, **kwargs):
    if not isinstance(value, list):
        raise TypeError('terms() requires a list as value.')

    output = {'terms': {field: value}}
    output['terms'].update(kwargs)
    return output


def _term_param(_term_type, *params, **kwargs):
    value = kwargs.get('value')
    term_ = None

    # assume field / value
    if value is None and len(params) == 2:
        term_ = params[0]
        value = params[1]
    elif value and len(params) == 1 and isinstance(params[0], string_types):
        term_ = params[0]

    # if the term and value are strings
    if isinstance(value, string_types) and isinstance(term_, string_types):
        # this should probably be deprecated. Normally we want match()
        # assume field / value
        return {_term_type: term(term_, value)}
    elif len(params) == 1:
        # single parameter with no value should be a list or dict
        return {_term_type: params[0]}
    else:
        # this is normally being called to wrap other QDSL calls in list form
        return {_term_type: list(params)}


def must(*args, **kwargs):

    return _term_param("must", *args, **kwargs)


def must_not(*args, **kwargs):
    return _term_param("must_not", *args, **kwargs)


def should(*args, **kwargs):
    return _term_param("should", *args, **kwargs)


def boosting(*args, **kwargs):
    output = {"boosting": {}}

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


def positive(params, value):
    return _term_param('positive', params, value)


def negative(params, value):
    return _term_param('negative', params, value)


def common(field, value, **kwargs):
    output = {"common": {field: {"query": value}}}
    output['common'][field].update(kwargs)
    return output


def constant_score(*args, **kwargs):
    output = {'constant_score': {}}
    if len(args) == 1 and isinstance(args[0], dict):
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


# mostly for completeness. Not much magic going on here
def function_score(*args, **kwargs):
    output = {'function_score': {}}

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
    return {'fuzzy': {field: value}}


def ids(*values, **kwargs):
    if isinstance(values[0], list):
        values = values[0]
    elif len(values) == 1 and not isinstance(values, list):
        values = [values[0]]  # to get rid of set
    else:
        values = list(values)

    output = {'ids': {'values': values}}
    if kwargs.get('type'):
        output['ids']['type'] = kwargs.get('type')

    return output


def query_term(field, value):
    return _term_param('query', field, value)


def indices(*args, **kwargs):
    output = {'indices': {}}
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

        output['indices'].update(kwargs)

    return output


def match_all(**kwargs):
    output = {"match_all": {}}
    output['match_all'].update(kwargs)
    return output


def more_like_this(*args, **kwargs):
    output = {"more_like_this": {}}
    if len(args) == 1 and isinstance(args[0], dict):
        output['more_like_this'] = args[0]

    output['more_like_this'].update(kwargs)

    return output


def nested(*args, **kwargs):
    output = {"nested": {}}

    if len(args) == 1 and isinstance(args[0], dict):
        output['nested'] = args[0]

    output['nested'].update(kwargs)

    return output


def prefix(field, value=None, **kwargs):
    output = {"prefix": {}}
    if isinstance(field, dict):
        output['prefix'] = field
    else:
        output['prefix'] = {field: {'value': value}}

        output['prefix'][field].update(kwargs)

    return output


def _query_string_output(_type, string, **kwargs):

    output = {_type: {"query": string}}
    output[_type].update(kwargs)

    return output


def query_string(string, **kwargs):
    return _query_string_output('query_string', string, **kwargs)


def simple_query_string(string, **kwargs):
    return _query_string_output('simple_query_string', string, **kwargs)


def range_(field, **kwargs):
    output = {"range": {}}

    if isinstance(field, dict):
        output['range'] = field
    else:
        output['range'][field] = kwargs

    return output


def regexp(field, regex=None, **kwargs):
    output = {'regexp': {}}
    if isinstance(field, dict):
        output['regexp'] = field
    else:
        output = {'regexp': {field: {'value': regex}}}
        output['regexp'][field].update(kwargs)

    return output


def span_term(field, value=None, **kwargs):
    if isinstance(field, dict):
        field.update(kwargs)
        output = {'span_term': field}
    else:
        output = {"span_term": {field: {'value': value}}}
        output['span_term'][field].update(kwargs)

    return output


def span_first(arg=None, **kwargs):
    output = {'span_first': {}}

    if arg and not isinstance(arg, dict):
        raise TypeError('Argument to span_first() must be dict (ether a term or another span query, or a match)')

    found_arg = False
    if arg:
        if 'match' in arg:
            output['span_first'] = arg
            found_arg = True
        else:
            for qtype in ['span_term', 'span_multi', 'span_near', 'span_not', 'span_or', 'span_term']:
                if qtype in arg:
                    found_arg = True
                    output['span_first']['match'] = arg
                    break

    if 'end' in kwargs:
        output['span_first']['end'] = kwargs.get('end')
        del kwargs['end']

    if not found_arg:
        output['span_first']['match'] = {}
        for k, v in iteritems(kwargs):
            output['span_first']['match'] = span_term(k, v)
            break

    return output


def span_multi(_match):
    if isinstance(_match, dict):
        if 'match' in _match:
            _match = _match['match']

        output = {"span_multi": {'match': _match}}
        return output
    else:
        raise TypeError('Argument to span_multi() must be dict')


def _build_clause_span(span_type, *clauses, **kwargs):
    if len(clauses) == 1 and isinstance(clauses[0], list):
        clauses = clauses[0]
    else:
        clauses = list(clauses)

    if all(isinstance(x, dict) for x in clauses):
        output = {span_type: {'clauses': clauses}}
        output[span_type].update(kwargs)

        return output
    else:
        raise TypeError('Arguments to {}() must be dict or list of dicts'.format(span_type))


def span_near(*clauses, **kwargs):
    return _build_clause_span('span_near', *clauses, **kwargs)


def span_or(*clauses, **kwargs):
    return _build_clause_span('span_or', *clauses, **kwargs)


def span_not(include=None, exclude=None):
    output = {"span_not": {}}
    if include:
        if not isinstance(include, dict):
            raise TypeError('include parameter must be a dict')

        output["span_not"]["include"] = include

    if exclude:
        if not isinstance(exclude, dict):
            raise TypeError('exclude parameter must be a dict')

        output["span_not"]["exclude"] = exclude
    return output


def _build_span_big_little(span_type, little=None, big=None):
    output = {}
    if little:
        if not isinstance(little, dict):
            raise TypeError('little parameter must be a dict')
        output['little'] = little

    if big:
        if not isinstance(big, dict):
            raise TypeError('big parameter must be a dict')

        output['big'] = big

    return {span_type: output}


def span_containing(little=None, big=None):
    return _build_span_big_little('span_containing', little=little, big=big)


def span_within(little=None, big=None):
    return _build_span_big_little('span_within', little=little, big=big)


def field_masking_span(span_query, field=None):
    if not isinstance(span_query, dict):
        raise TypeError('query argument must be a dict')

    output = {}
    if 'query' in span_query:
        output = span_query
    else:
        output['query'] = span_query
        if field:
            output['field'] = field

    return {'field_masking_span': output}


def wildcard(field, value, **kwargs):
    output = {"wildcard": {field: {'value': value}}}
    output['wildcard'][field].update(kwargs)
    return output


def exists(field):
    return {"exists": {"field": field}}


def geo_bounding_box(field, **kwargs):
    if isinstance(field, dict):
        return {"geo_bounding_box": field}
    else:
        return {"geo_bounding_box": {field: kwargs}}


def geo_distance(field, point=None, distance=None, **kwargs):
    if isinstance(field, dict):
        output = field
    else:
        if not point or not distance:
            raise TypeError('point and distance parameters are required unless field (param 1) is dict')

        output = {"distance": distance, field: point}
    output.update(kwargs)

    return {"geo_distance": output}


def geo_range(field, point=None, from_dist=None, to_dist=None, **kwargs):
    if isinstance(field, dict):
        output = field
    else:
        if not point or not from_dist or not to_dist:
            raise TypeError('point, from_dist, and to_dist must be set if field is not dict')

        output = {field: point, "from": from_dist, "to": to_dist}
        output.update(kwargs)

    return {'geo_distance_range': output}


geo_distance_range = geo_range  # alias


def geo_polygon(field, points=None, **kwargs):
    if isinstance(field, dict):
        output = field
    else:
        if not isinstance(points, list):
            raise TypeError('points must be list')
        output = {field: {'points': points}}

        output.update(kwargs)
    return {"geo_polygon": output}


def geo_shape(field, shape=None, indexed_shape=None, **kwargs):
    if isinstance(field, dict):
        output = field
    else:
        output = {field: {}}
        if shape:
            output[field]['shape'] = shape
        elif indexed_shape:
            output[field]['indexed_shape'] = indexed_shape
        else:
            raise TypeError('geo_shape requires a shape or indexed_shape')

        output[field].update(kwargs)

    return {'geo_shape': output}


def geohash_cell(field, lat, lon, **kwargs):
    output = {"geohash_cell": {field: {"lat": lat, "lon": lon}}}
    output['geohash_cell'].update(kwargs)
    return output


def _parent_child_query(query_type, _type, _query, **kwargs):
    if isinstance(_type, dict):
        output = _type
    else:
        if _query is None:
            raise TypeError('query is required unless first parameter is dict')
        output = {'type': _type, 'query': _query}
        output.update(kwargs)
    return {query_type: output}


def has_child(_type, query=None, **kwargs):
    return _parent_child_query('has_child', _type, query, **kwargs)


def has_parent(_type, query=None, **kwargs):
    return _parent_child_query('has_parent', _type, query, **kwargs)


def missing(field):
    return must_not(exists(field))


def script(source, lang=None, **kwargs):
    if isinstance(source, dict):
        output = source
    else:
        if not source or not lang:
            raise TypeError('source and lang are required unless first parameter is dict')

        output = {'source': source, 'lang': lang}

    # do this twice because for some weird reason the QDSL syntax
    # is 'script': { 'script': {...}}
    for x in range(2):
        if 'script' not in output or 'script' not in output['script']:
            output = {'script': output}

    output['script']['script'].update(kwargs)
    return output


def type_(_type):
    return {"type": {"value": _type}}


def highlight(fields=None, **kwargs):
    if not fields:
        fields = {}
    fields.update(kwargs)

    return {"highlight": {'fields': fields}}
