import types
import copy
from datetime import date, datetime
from uuid import uuid4
import json

from elasticsearch import NotFoundError, helpers, client

from . import config, querybuilder, qdsl
from .connection import VWConnection
from .config import logger
from .util import unset, all_subclasses
from .es_types import *  # implements elastic search types


class ObjectDeletedError(Exception):
    pass


# Raised when no results are found for one()
class NoResultsFound(Exception):
    pass


# Implements callbacks across objects
class VWCallback(object):
    _callbacks = {}

    @classmethod
    def register_callback(cls, cbtype, callback):
        if cls.__name__ not in cls._callbacks:
            cls._callbacks[cls.__name__] = {}

        if cbtype not in cls._callbacks[cls.__name__]:
            cls._callbacks[cls.__name__][cbtype] = []

        if not callable(callback):
            raise ValueError('parameter 2 to register_callback() must be \
                callable')

        cls._callbacks[cls.__name__][cbtype].append(callback)

    @classmethod
    def deregister_callback(cls, cbtype, callback_name):
        try:
            for cb in cls._callbacks[cls.__name__][cbtype]:
                if cb == callback_name or cb.__name__ == callback_name:
                    cls._callbacks[cls.__name__][cbtype].remove(cb)
                    break
        except KeyError:
            pass

    def execute_callbacks(self, cbtype, argument=None, **kwargs):
        try:
            for cb in self._callbacks[self.__class__.__name__][cbtype]:
                argument = cb(self, argument, **kwargs)
        except KeyError:
            pass  # no callbacks by this name.

        return argument

    @classmethod
    def execute_class_callbacks(cls, cbtype, argument=None, **kwargs):
        try:
            for cb in cls._callbacks[cls.__name__][cbtype]:
                argument = cb(argument, **kwargs)
        except KeyError:
            pass  # no callbacks by this name.

        return argument


class VWBase(VWCallback):
    _watch = False
    _needs_update = False
    id = ''
    __index__ = None

    def __init__(self, _set_by_query=False, **kwargs):
        # the internal document
        self._document = {}

        # pickling off by default. Set by __getstate__ and __setstate__ when
        # the object is pickled/unpickled. Allows all values to be set
        self._pickling = False

        # TODO this flag is no longer used due to the removal of relationships
        # but may be useful in the future so it is here for now.
        self._no_ex = True

        if _set_by_query:
            self._new = False
            self._set_by_query = True
        else:
            self._new = True
            self.execute_callbacks('before_manual_create_model')

        self._needs_update = False
        self._watch = True

        self._deleted = False

        if self.__index__ is None:
            self.__index__ = config.default_index

        for k in dir(self):
            v = getattr(self, k)
            # skip functions and special variables
            if not isinstance(v, types.MethodType) and not k[0] == '_':

                # check if we were called with a variable. If so set
                try:
                    v = kwargs[k]
                except KeyError:
                    pass

                setattr(self, k, v)

        if 'id' not in kwargs:
            self.id = str(uuid4())

        # make sure we're ready for changes
        self._set_by_query = False
        self._no_ex = False
        if self._new:
            self.execute_callbacks('after_manual_create_model')

    # customizations for pickling
    def __getstate__(self):
        # mark as pickling
        self._pickling = True

        # copy the __dict__. Need copy so we don't
        # break things when flags are removed
        retval = {}

        for k, v in self.__dict__.iteritems():
            if k != '_es' and k != '_pickling':
                retval[k] = copy.deepcopy(v)

        self._pickling = False
        return retval

    def __setstate__(self, state):
        self._pickling = True

        for k, v in state.iteritems():
            setattr(self, k, v)

        self._pickling = False

    def __getattribute__(self, name):
        # ok this is much funky
        no_ex = False
        try:
            no_ex = super(VWBase, self).__getattribute__('_no_ex')
        except AttributeError:
            pass

        v = unset
        doc = None
        try:
            doc = super(VWBase, self).__getattribute__('_document')
            if name in doc:
                v = doc.get(name, unset)
        except AttributeError:
            pass

        if not v:
            default_v = super(VWBase, self).__getattribute__(name)

            # list and dict objects attributes cannot be set to None. Others can
            if v is unset or isinstance(default_v, list) or isinstance(default_v, dict):
                v = default_v

            # instance attribute was becoming a reference to the class
            # attribute. Not what we wanted, make a copy
            if doc:
                if not isinstance(v, types.MethodType) and not name[0] == '_':
                    v = copy.deepcopy(v)
                    self._document[name] = v
                    return self._document[name]

        if isinstance(v, basestring):
            try:
                try:
                    return datetime.strptime(v, '%Y-%m-%dT%H:%M:%S')
                except ValueError:
                    return datetime.strptime(v, '%Y-%m-%d').date()
            except (ValueError, AttributeError, TypeError):
                return v
        else:
            return v

    def __get_current_value(self, name):
        try:
            return super(VWBase, self).__getattribute__(name)
        except AttributeError:
            return None

    def __set_document_value(self, name, value):

        if name[0] == '_':
            # special rules for names with underscores.
            # setting the _ values will not trigger an update.
            if (name not in dir(self) or name in ['_set_by_query', '_deleted', '_watch', '_new', '_no_ex', '_pickling',
                                                  '_document', '_callbacks'] or self._pickling):
                object.__setattr__(self, name, value)  # not copied
        else:
            curr_value = create_es_type(self.__get_current_value(name))  # create as an es_type

            try:
                if value.__metaclass__ == ESType:
                    set_value_cls = False
                elif value is None:
                    set_value_cls = False
                else:
                    set_value_cls = True
            except AttributeError:
                if value is None:
                    set_value_cls = False
                else:
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
                    if curr_value.__metaclass__ == ESType:
                        cls = curr_value.__class__
                        params = curr_value.es_args()

                        # try to set the value as the same class.
                        try:
                            value = cls(value, **params)
                        except:
                            # value didn't set. Try set as es_type
                            test_value = create_es_type(value)

                            # dates and times are special
                            if isinstance(test_value, DateTime):
                                if isinstance(curr_value, DateTime):
                                    value = DateTime(test_value.year,
                                                     test_value.month, test_value.day,
                                                     test_value.hour, test_value.minute,
                                                     test_value.second,
                                                     test_value.microsecond,
                                                     test_value.tzinfo, **params)
                                elif isinstance(curr_value, Date):
                                    value = Date(test_value.year,
                                                 test_value.month, test_value.day,
                                                 **params)
                                else:
                                    value = test_value
                            else:
                                value = test_value

                        if type_enforcement:
                            try:
                                if value.__class__ != curr_value.__class__:
                                    raise TypeError('strict type enforcement is enabled. %s must be set with %s' % (
                                        name, curr_value.__class__.__name__))
                            except:
                                # errors where value isn't even a class
                                # will raise their own exception.
                                # Catch here to avoid attribute errors
                                # from this block being passed along below
                                raise

                except AttributeError:
                    # curr_value couldn't be converted to an ESType
                    # we just fall back to regular types.
                    # if ES has an issue it will throw its own exception.
                    pass

            # just set the field on the document
            if isinstance(value, DateTime) or isinstance(value, datetime):
                self._document[name] = value.strftime('%Y-%m-%dT%H:%M:%S')
            elif isinstance(value, Date) or isinstance(value, date):
                self._document[name] = value.strftime('%Y-%m-%d')
            elif isinstance(value, Boolean):
                self._document[name] = bool(value)
            else:
                self._document[name] = value

            if self._watch:
                object.__setattr__(self, '_needs_update', True)
                object.__setattr__(self, '_watch', False)

    def __setattr__(self, name, value):
        if '_deleted' in dir(self) and self._deleted:
            raise ObjectDeletedError

        self.__set_document_value(name, value)

    def commit(self, connection=None, **kwargs):
        # save in the db
        if not connection:
            connection = VWConnection.get_connection()

        if self._deleted and hasattr(self, 'id') and self.id:
            self.execute_callbacks('on_delete')

            connection.delete(id=self.id, index=self.__index__, doc_type=self.__type__)
        else:
            self.execute_callbacks('before_commit')
            idx = self.__index__
            doc_type = self.__type__

            doc = self._document

            kwargs.update({
                'index': idx,
                'doc_type': doc_type,
                'body': doc,
            })
            if hasattr(self, 'id') and self.id:
                kwargs['id'] = self.id

            connection.index(**kwargs)
            self._watch = True
            self.execute_callbacks('after_commit')

    def sync(self, connection=None):
        if not connection:
            connection = VWConnection.get_connection()

        if self.id:
            try:
                self.execute_callbacks('before_sync')
                res = connection.get(id=self.id, index=self.__index__)
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

    # to dict is for overriding. _create_source_document() should never be
    # overridden!
    def to_dict(self):
        # copy so we don't overwrite the original document
        return copy.deepcopy(self._create_source_document(datetime_format='%Y-%m-%d %H:%M:%S'))

    def _create_source_document(self, **kwargs):
        output = self._document
        return output

    def more_like_this(self, **kwargs):
        c = VWCollection(base_obj=self.__class__)
        return c.get_like_this(self.id).all(**kwargs)

    def collection(self):
        """
        Returns an instantiated VWCollection class that has the model provided in base_obj.
        @param base_obj: class
        @return: VWCollection
        """
        for vwcollection in all_subclasses(VWCollection):
            try:
                if vwcollection.__model__ is self.__class__:
                    return vwcollection()
            except AttributeError:
                pass

        return VWCollection(base_obj=self.__class__)


# setup the collections
class VWCollection(VWCallback):
    __model__ = None

    def __init__(self, items=None, **kwargs):
        self._items = items or []  # special list of items that can be committed in bulk
        self.bulk_chunk_size = kwargs.get('bulk_chunk_size', config.bulk_chunk_size)
        self.results_per_page = kwargs.get('results_per_page', config.results_per_page)
        self.base_obj = kwargs.get('base_obj', self.__class__.__model__)
        if self.base_obj is None:
            raise AttributeError('Base object must contain a model or pass base_obj')

        self.type = self.base_obj.__type__
        self.idx = getattr(self.base_obj, '__index__', config.default_index)

        self._sort = []
        self._raw = {}
        self._special_body = {}
        self._querybody = querybuilder.QueryBody()  # sets up the new query bodies

    def search(self, query, **kwargs):
        self._querybody.chain(qdsl.query_string(query, **kwargs), type='query')
        return self

    # setup a raw request
    def raw(self, raw_request):
        self._raw = raw_request
        return self

    def _check_datetime(self, value):
        if isinstance(value, datetime):
            return value.isoformat()
        elif isinstance(value, date):
            return value.strftime('%Y-%m-%d')
        else:
            return value

    def _check_datetime_dict(self, kwargs_dict):
        for k, v in kwargs_dict.iteritems():
            kwargs_dict[k] = self._check_datetime(v)

        return kwargs_dict

    def filter_by(self, parameters=None, condition='and', **kwargs):

        condition = self._translate_bool_condition(condition)

        if not isinstance(parameters, dict):
            parameters = {}

        parameters.update(kwargs)

        for k, v in parameters.iteritems():
            if isinstance(v, list):
                v = [self._check_datetime(vi) for vi in v]
            else:
                v = self._check_datetime(v)

            id_filter = []

            if k == 'id' or k == 'ids':
                id_filter = v
                if not isinstance(id_filter, list):
                    id_filter = [id_filter]

                id_filter = [_id for _id in id_filter if _id is not None]

                if len(id_filter) < 1:
                    raise ValueError('%s keyword must not be empty' % k)

            if len(id_filter) > 0:

                self._querybody.chain(qdsl.ids(id_filter), condition=condition)
            else:
                try:
                    analyzed = is_analyzed(getattr(self.base_obj, k))
                except AttributeError:
                    analyzed = is_analyzed(v)

                q_type = 'filter'
                if analyzed:
                    q_type = 'query'

                if isinstance(v, list):
                    # lists are treat as like "OR" (using terms() on not_analyzed, bool/matched on analyzed)
                    if analyzed:
                        match_queries = []
                        for item in v:
                            match_queries.append(qdsl.match(k, item))
                        self._querybody.chain(qdsl.bool(qdsl.should(match_queries)), condition=condition, type=q_type)
                    else:
                        self._querybody.chain(qdsl.terms(k, v), condition=condition,
                                              type=q_type)
                else:
                    # search_value = unicode(v)
                    if analyzed:
                        self._querybody.chain(qdsl.match(unicode(k), v), condition=condition, type=q_type)
                    else:
                        self._querybody.chain(qdsl.term(unicode(k), v), condition=condition, type=q_type)

        return self

    def multi_match(self, fields, query, **kwargs):
        condition = kwargs.get('condition', None)

        try:
            del kwargs['condition']
        except KeyError:
            pass

        kwargs = self._check_datetime_dict(kwargs)

        self._querybody.chain(qdsl.multi_match(query, fields, **kwargs), condition=condition, type='query')
        return self

    def exact(self, field, value, **kwargs):
        kwargs = self._check_datetime_dict(kwargs)
        try:
            field_template = getattr(self.base_obj, field)

            if type(field_template) != ESType:
                field_template = create_es_type(field_template)

            for estype in [String, IP, Attachment]:
                if isinstance(field_template, estype) and field_template.analyzed == True:
                    logger.warn('%s types may not exact match correctly if they are analyzed' % unicode(
                        estype.__class__.__name__))

        except AttributeError:
            logger.warn('%s is not in the base model.' % unicode(field))

        kwargs['type'] = 'filter'
        if isinstance(value, list):
            self._querybody.chain(qdsl.terms(field, value), **kwargs)
        else:
            self._querybody.chain(qdsl.term(field, value), **kwargs)

        return self

    def or_(self, *args):
        return ' OR '.join(args)

    def and_(self, *args):
        return ' AND '.join(args)

    def get(self, id, connection=None, **kwargs):
        if not connection:
            connection = VWConnection.get_connection()

        try:
            params = {'index': self.idx, 'doc_type': self.type, 'id': id}
            params.update(kwargs)
            doc = connection.get(**params)
            if doc:
                return VWCollectionGen(self.base_obj, {'docs': [doc]})[0]

            return None

        except NotFoundError:
            return None

    def refresh(self, connection=None, **kwargs):
        if not connection:
            connection = VWConnection.get_connection()

        es_client = client.IndicesClient(connection)
        es_client.refresh(index=self.idx, **kwargs)

    def get_in(self, ids, connection=None, **kwargs):
        if not connection:
            connection = VWConnection.get_connection()

        if len(ids) > 0:  # check for ids. empty list returns an empty list (instead of exception)
            # filter any Nones in the list as they crash the client
            ids = [_id for _id in ids if _id != None]
            if len(ids) < 1:
                return []

            params = {'index': self.idx, 'doc_type': self.type, 'body': {'ids': ids}}
            params.update(kwargs)

            res = connection.mget(**params)
            if res and res.get('docs'):
                return VWCollectionGen(self.base_obj, res)

        return []

    def get_like_this(self, doc_id, connection=None, **kwargs):
        if not connection:
            connection = VWConnection.get_connection()

        params = {'index': self.idx, 'doc_type': self.type, 'id': doc_id}
        params.update(kwargs)
        res = connection.mlt(**params)

        if res and res.get('docs'):
            return VWCollectionGen(self.base_obj, res)
        else:
            return []

    def sort(self, **kwargs):
        for k, v in kwargs.iteritems():
            v = v.lower()
            if v not in ['asc', 'desc']:
                v = 'asc'

            self._sort.append('%s:%s' % (k, v))
        return self

    def clear_previous_search(self):
        self._raw = {}
        self._special_body = {}
        self._querybody = querybuilder.QueryBody()
        self._sort = []

    def _create_search_params(self, **kwargs):
        # before_query_build() is allowed to manipulate the object's internal state before we do stuff
        self._querybody = self.execute_callbacks('before_query_build', self._querybody)

        q = {'index': self.idx, 'doc_type': self.type}

        if self._raw:
            q['body'] = self._raw
        elif self._querybody:
            q['body'] = self._querybody.build()
        else:
            q['body'] = qdsl.query(qdsl.match_all())

        # after_query_build() can manipulate the final query before being sent to ES
        # this is generally considered a bad idea but might be useful for logging
        q = self.execute_callbacks('after_query_build', q)

        logger.debug(json.dumps(q))
        return q

    def count(self, connection=None):
        if not connection:
            connection = VWConnection.get_connection()

        params = self._create_search_params()
        resp = connection.count(**params)
        return resp.get('count')

    def __len__(self):
        return self.count()

    def limit(self, count):
        self.results_per_page = count
        return self

    def all(self, connection=None, **kwargs):
        if not connection:
            connection = VWConnection.get_connection()

        logger.info(connection)

        params = self._create_search_params()
        if not params.get('size'):
            params['size'] = self.results_per_page

        if kwargs.get('results_per_page') != None:
            kwargs['size'] = kwargs.get('results_per_page')
            del kwargs['results_per_page']

        if kwargs.get('start') != None:
            kwargs['from_'] = kwargs.get('start')
            del kwargs['start']

        logger.debug(json.dumps(self._sort))

        params.update(kwargs)
        if len(self._sort) > 0:
            if params.get('sort') and isinstance(params['sort'], list):
                params['sort'].extend(self._sort)
            else:
                params['sort'] = self._sort

        if params.get('sort'):
            if isinstance(params['sort'], list):
                params['sort'] = ','.join(params.get('sort'))
            else:
                raise TypeError('"sort" argument must be a list')

        logger.debug(json.dumps(params))
        results = connection.search(**params)

        return VWCollectionGen(self.base_obj, results)

    def one(self, **kwargs):
        kwargs['results_per_page'] = 1
        results = self.all(**kwargs)
        try:
            return results[0]
        except IndexError:
            raise NoResultsFound('No result found for one()')

    # this is for legacy purposes in filter_by
    def _translate_bool_condition(self, _bool_condition):
        if _bool_condition == 'and':
            _bool_condition = 'must'
        elif _bool_condition == 'or':
            _bool_condition = 'should'
        elif _bool_condition == 'not':
            _bool_condition = 'must_not'

        return _bool_condition

    def range(self, field, **kwargs):
        search_options = {}
        for opt in ['condition', 'minimum_should_match']:
            if opt in kwargs:
                search_options[opt] = kwargs.get(opt)
                del kwargs[opt]

        kwargs = self._check_datetime_dict(kwargs)

        q = qdsl.range(field, **kwargs)
        if self._querybody.is_filtered():
            d = {'filter': q}
        else:
            d = {'query': q}

        if search_options:
            d.update(search_options)

        self._querybody.chain(d)

        return self

    def search_geo(self, field, distance, lat, lon, **kwargs):
        condition = kwargs.get('condition', 'and')
        if 'condition' in kwargs:
            del kwargs['condition']

        self._querybody.chain(qdsl.filter_(qdsl.geo_distance(field, [lon, lat], distance, **kwargs)),
                              condition=condition)
        return self

    def missing(self, field, **kwargs):
        self._querybody.chain(qdsl.filter_(qdsl.missing(field)))
        return self

    def exists(self, field, **kwargs):
        self._querybody.chain(qdsl.filter_(qdsl.exists(field, **kwargs)))
        return self

    def delete(self, connection=None):
        deletes = self.all(size=self.count(), _source_include=['id']).results()['hits']['hits']
        ids = [d['_source'].get('id') for d in deletes]
        return self.delete_in(ids, connection)

    def delete_in(self, ids, connection=None):
        if not connection:
            connection = VWConnection.get_connection()

        if not isinstance(ids, list):
            raise TypeError('argument to delete in must be a list.')

        bulk_docs = []
        for i in ids:
            this_id = i
            this_type = self.base_obj.__type__
            this_idx = self.idx
            if isinstance(i, VWBase):
                this_id = i.id
                this_type = i.__type__
                try:
                    this_idx = i.__index__
                except AttributeError:
                    pass

            bulk_docs.append({'_op_type': 'delete', '_type': this_type, '_index': this_idx, '_id': this_id})

        return helpers.bulk(connection, bulk_docs, chunk_size=self.bulk_chunk_size)

    # commits items in bulk
    def commit(self, callback=None, connection=None):
        if not connection:
            connection = VWConnection.get_connection()

        bulk_docs = []

        if callback:
            if not callable(callback):
                raise TypeError('Argument 2 to commit() must be callable')

        # allow for a search to work if there are not _items
        if len(self._items) == 0:
            items = self.all()
        else:
            items = self._items

        for i in items:
            if callback:
                i = callback(i)

            i = self.execute_callbacks('on_bulk_commit', i)

            if isinstance(i, VWBase):
                this_dict = i._create_source_document()
                this_type = i.__type__
                this_id = i.id
                try:
                    this_idx = i.__index__
                except AttributeError:
                    this_idx = self.idx
            elif isinstance(i, dict):
                this_dict = i
                this_id = i.get('id')
                this_idx = self.idx
                this_type = self.type
            else:
                raise TypeError('Elements passed to the collection must be type of "dict" or "VWBase"')

            if not this_id:
                this_id = str(uuid4())

            bulk_docs.append(
                {'_op_type': 'index', '_type': this_type, '_index': this_idx, '_id': this_id, '_source': this_dict})

        return helpers.bulk(connection, bulk_docs, chunk_size=self.bulk_chunk_size)


class VWCollectionGen(VWCallback):
    def __init__(self, base_obj, es_results):
        self.es_results = es_results

        try:
            self.doc_list = self.es_results['docs']
        except KeyError:
            try:
                self.doc_list = self.es_results['hits']['hits']
            except KeyError:
                raise ValueError('Results passed do not appear to be valid ElasticSearch results.')

        self.count = 0
        self.base_obj = base_obj

    def __iter__(self):
        return self

    # makes this python 3 compatible
    def __next__(self):
        return self.next()

    def next(self):
        self.count += 1
        if self.count > len(self.doc_list):
            raise StopIteration

        doc = self.doc_list[self.count - 1]

        # sometimes ES will return a "document" that has no _source
        # this is a hack to skip it
        if not doc.get('_source'):
            return self.next()

        return self._create_obj(doc)

    def _create_obj(self, doc):
        doc = self.base_obj.execute_class_callbacks('before_auto_create_model', doc)

        src = doc.get('_source')
        src['_set_by_query'] = True
        src['id'] = doc.get('_id')

        obj = self.base_obj(**src)
        return obj.execute_callbacks('after_auto_create_model', obj, **src)

    # python abuse!
    # seriously though we want to act like a list in many cases
    def __getitem__(self, idx):
        return self._create_obj(self.doc_list[idx])

    def __len__(self):
        return len(self.doc_list)

    def results(self):
        return self.es_results
