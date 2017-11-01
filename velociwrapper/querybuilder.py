from __future__ import absolute_import, unicode_literals
import copy
from . import qdsl

__all__ = ['QueryBody']


class QueryBody(object):
    def __init__(self):
        self._filter = {'must': [], 'should': [], 'must_not': []}
        self._query = {'must': [], 'should': [], 'must_not': []}
        self._bool = 'must'
        self._last_part = '_query'

    def __bool__(self):
        # Python 3.x version of __nonzero__
        return bool(self.is_filtered() or self.is_query())

    def __nonzero__(self):
        # Python 2.x version of __bool__
        return self.__bool__()

    def chain(self, newpart, **kwargs):
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

        # existing conditions in the dictionary will just get parsed recursively
        _condition = 'must'
        if kwargs.get('condition') in ['must', 'should', 'must_not']:
            _condition = kwargs.get('condition')
        elif isinstance(newpart, dict):
            _chained = False
            for lt in ['must', 'should', 'must_not']:
                if lt in newpart:
                    kwargs['condition'] = lt
                    self.chain(newpart[lt], **kwargs)
                    _chained = True
            if _chained:
                return self
        # else treat the condition as 'must'

        # chain the newpart to the toplevel bool
        if self._last_part == '_filter':
            if _condition not in self._filter:
                self._filter[_condition] = []

            self._filter[_condition].append(newpart)
        else:
            if _condition not in self._query:
                self._query[_condition] = []

            self._query[_condition].append(newpart)

        return self

    def is_filtered(self):
        for t in ['must', 'should', 'must_not']:
            if len(self._filter[t]) > 0:
                return True

    def is_query(self):
        for t in ['must', 'should', 'must_not']:
            if len(self._query[t]) > 0:
                return True

    def build(self):
        is_filtered = False
        is_query = False
        filter_is_multi_condition = False
        filter_needs_bool = False
        query_needs_bool = False

        f_type_count = 0
        q_type_count = 0

        # gets set to the last detected type
        # used if type_counts end up being one 
        # to quickly access the 
        q_type = None
        f_type = None

        # copy the filters and queries so the chain is still intact. Need so collections act the same as before
        _query = copy.deepcopy(self._query)
        _filter = copy.deepcopy(self._filter)

        for t in ['must', 'should', 'must_not']:
            try:
                if len(_filter[t]) > 0:
                    is_filtered = True
                    f_type_count += 1
                    f_type = t
                    if len(_filter[t]) == 1:
                        _filter[t] = _filter[t][0]  # if only one remove the list
                        if t == "must_not":
                            filter_needs_bool = True  # still need if "must_not"
                    else:
                        filter_needs_bool = True
                else:
                    del _filter[t]
            except KeyError:
                pass

            try:
                if len(_query[t]) > 0:
                    is_query = True
                    q_type_count += 1
                    q_type = t
                    if len(_query[t]) == 1:
                        _query[t] = _query[t][0]  # if only one remove the list

                        # if this is "must_not" we still need to declare bool
                        if t == 'must_not':
                            query_needs_bool = True
                    else:
                        query_needs_bool = True
                else:
                    del _query[t]
            except KeyError:
                pass

        if f_type_count > 1:
            filter_needs_bool = True
            filter_is_multi_condition = True

        if q_type_count > 1:
            query_needs_bool = True

        if is_query:
            if query_needs_bool:
                _output_query = {'bool': _query}
            else:
                _output_query = _query[q_type]

            _output_query = {'query': _output_query}
        else:
            _output_query = {'query': qdsl.match_all()}

        if is_filtered:
            if filter_needs_bool:
                _output_filter = {'bool': _filter}
            elif filter_is_multi_condition or isinstance(_filter[f_type], list):
                _output_filter = _filter  # explicit queries
            else:
                _output_filter = _filter[f_type]

            if not _output_query.get('bool'):
                _output_query = qdsl.bool_(qdsl.must(_output_query))

            _output_query['bool']['filter'] = _output_filter

        return _output_query
