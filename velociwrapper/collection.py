from uuid import uuid4
import json
import types
import copy
import logging
from datetime import date,datetime

from elasticsearch import Elasticsearch, NotFoundError,helpers, client

from . import config, querybuilder, qdsl
from .config import logger
from .es_types import *
from .base import VWBase,VWCallback

# Raised when no results are found for one()
class NoResultsFound(Exception):
    pass

class VWCollection(VWCallback):
    
    def __init__(self,items=[],**kwargs):
        self.bulk_chunk_size = kwargs.get('bulk_chunk_size',
            config.bulk_chunk_size)
        self._sort = []
        self.results_per_page = kwargs.get('results_per_page',
            config.results_per_page)
        self._querybody = querybuilder.QueryBody() # sets up the new query bodies

        if kwargs.get('base_obj'):
            self.base_obj = kwargs.get('base_obj')
        else:
            try:
                self.base_obj = self.__class__.__model__
            except AttributeError:
                raise AttributeError('Base object must contain a model or pass base_obj')

        self._es = Elasticsearch(config.dsn)
        self._esc = client.IndicesClient(self._es)

        if '__index__' in dir(self.base_obj):
            idx = self.base_obj.__index__
        else:
            idx = config.default_index

        self._search_params = []
        self._raw = {}
        self.idx = idx
        self.type = self.base_obj.__type__
        self._special_body = {}
        
        # special list of items that can be committed in bulk
        self._items = items 

    def search(self,q):
        self._search_params.append(q)
        return self

    # setup a raw request
    def raw(self, raw_request):
        self._raw = raw_request
        return self

    def filter_by(self, condition = 'and',**kwargs):
        if kwargs.get('condition'):
            condition=kwargs.get('condition')
            del kwargs['condition']

        condition = self._translate_bool_condition(condition)

        for k,v in kwargs.iteritems():
            if k == 'id' or k == 'ids':
                id_filter = v
                if not isinstance(id_filter, list):
                    id_filter = [id_filter]

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
                    # lists are treat as like "OR" (using terms())
                    self._querybody.chain( qdsl.terms(k,v),condition=condition,
                        type=q_type)
                else:
                    #search_value = unicode(v)
                    if analyzed:
                        self._querybody.chain(qdsl.match(unicode(k), v), condition=condition,type=q_type)
                    else:
                        self._querybody.chain(qdsl.term(unicode(k), v), condition=condition,type=q_type)

        return self

    def multi_match(self, fields, query, **kwargs):
        self._querybody.chain(qdsl.multi_match(query, fields), condition=kwargs.get('condition', None), type='query')
        return self

    def exact(self, field, value,**kwargs):
        try:
            field_template = getattr( self.base_obj, field)

            if type(field_template) != ESType:
                field_template = create_es_type(field_template)

            for estype in [String,IP,Attachment]:
                if isinstance(field_template, estype) and field_template.analyzed == True:
                    logger.warn('%s types may not exact match correctly if they are analyzed' % unicode(estype.__class__.__name__))

        except AttributeError:
            logger.warn('%s is not in the base model.' % unicode(field))

        kwargs['type'] = 'filter'
        if isinstance(value, list):
            self._querybody.chain(qdsl.terms(field,value), **kwargs)
        else:
            self._querybody.chain(qdsl.term(field, value), **kwargs)

        return self


    def or_(self,*args):
        return ' OR '.join(args)

    def and_(self,*args):
        return ' AND '.join(args)

    def get(self,id, **kwargs):
        try:
            params = {'index':self.idx, 'doc_type':self.type, 'id':id}
            params.update(kwargs)
            doc = self._es.get(**params)
            if doc:
                return VWCollectionGen(self.base_obj, {'docs':[doc]})[0]

            return None

        except:
            # TODO. Discuss this. Should get() return None even on exceptions?
            return None

    def refresh(self, **kwargs):
        self._esc.refresh(index=self.idx, **kwargs)

    def get_in(self, ids,**kwargs):
        if len(ids) > 0: # check for ids. empty list returns an empty list (instead of exception)
            params = {'index':self.idx, 'doc_type':self.type, 'body':{'ids':ids}}
            params.update(kwargs);
            res = self._es.mget(**params)
            if res and res.get('docs'):
                return VWCollectionGen(self.base_obj, res)

        return []

    def get_like_this(self,doc_id,**kwargs):
        params = {'index':self.idx,'doc_type':self.type,'id':doc_id}
        params.update(kwargs)
        res = self._es.mlt(**params)

        if res and res.get('docs'):
            return VWCollectionGen(self.base_obj, res)
        else:
            return []

    def sort(self, **kwargs):
        for k,v in kwargs.iteritems():
            v = v.lower()
            if v not in ['asc','desc']:
                v = 'asc'

            self._sort.append('%s:%s' % (k,v))
        return self

    def clear_previous_search(self):
        self._raw = {}
        self._search_params = []
        self._special_body = {}
        self._querybody = querybuilder.QueryBody()

    def _create_search_params( self, **kwargs ):
        # before_query_build() is allowed to manipulate the object's internal state before we do stuff
        self._querybody = self.execute_callbacks('before_query_build', self._querybody )

        q = {
            'index': self.idx,
            'doc_type': self.type
        }

        if self._raw:
            q['body'] = self._raw
        elif len(self._search_params) > 0:
            kwargs['type'] = 'query'
            self._querybody.chain(qdsl.query_string(self.and_(*self._search_params)), **kwargs)
        else:
            q['body'] = qdsl.query(qdsl.match_all())

        if self._querybody.is_filtered() or self._querybody.is_query():
            q['body'] = self._querybody.build()

        # after_query_build() can manipulate the final query before being sent to ES
        # this is generally considered a bad idea but might be useful for logging
        q = self.execute_callbacks( 'after_query_build', q )

        logger.debug(json.dumps(q))
        return q

    def count(self):
        params = self._create_search_params()
        resp = self._es.count(**params)
        return resp.get('count')

    def __len__(self):
        return self.count()

    def limit(self,count):
        self.results_per_page = count
        return self

    def all(self,**kwargs):

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
        results = self._es.search(**params)

        return VWCollectionGen(self.base_obj,results)

    def one(self,**kwargs):
        kwargs['results_per_page'] = 1
        results = self.all(**kwargs)
        try:
            return results[0]
        except IndexError:
            raise NoResultsFound('No result found for one()')

    # this is for legacy purposes in filter_by
    def _translate_bool_condition(self,_bool_condition):
        if _bool_condition == 'and':
            _bool_condition = 'must'
        elif _bool_condition == 'or':
            _bool_condition = 'should'
        elif _bool_condition == 'not':
            _bool_condition = 'must_not'

        # this is for things like geo_distance where we explicitly want the true and/or/not
        elif _bool_condition == 'explicit_and':
            _bool_condition = 'and'
        elif _bool_condition == 'explicit_or':
            _bool_condition = 'or'
        elif _bool_condition == 'explicit_not':
            _bool_condition = 'not'

        return _bool_condition

    def range(self, field, **kwargs):
        search_options = {}
        for opt in ['condition','minimum_should_match']:
            if opt in kwargs:
                search_options[opt] = kwargs.get(opt)
                del kwargs[opt]

        q = qdsl.range(field, **kwargs)
        if self._querybody.is_filtered():
            d = {'filter': q}
        else:
            d = {'query': q}

        if search_options:
            d.update(search_options)

        self._querybody.chain(d)

        return self

    def search_geo(self, field, distance, lat, lon,**kwargs):
        condition = kwargs.get('condition', 'and')
        if 'condition' in kwargs:
            del kwargs['condition']

        self._querybody.chain(qdsl.filter_(qdsl.geo_distance(field, [lon,lat], distance, **kwargs)), condition=condition)
        return self

    def missing( self, field, **kwargs):
        self._querybody.chain(qdsl.filter_(qdsl.missing(field)))
        return self

    def exists( self, field, **kwargs):
        self._querybody.chain(qdsl.filter_(qdsl.exists(field, **kwargs)))
        return self

    def delete(self, **kwargs):
        params = self._create_search_params()
        params.update(kwargs)
        self._es.delete_by_query(**params)

    def delete_in(self, ids):
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

            bulk_docs.append({'_op_type': 'delete', '_type': this_type, '_index': this_idx, '_id': this_id })

        return helpers.bulk( self._es, bulk_docs, chunk_size=self.bulk_chunk_size)

    # commits items in bulk
    def commit(self, callback=None):
        bulk_docs = []

        if callback:
            if not callable(callback):
                raise TypeError('Argument 2 to commit() must be callable')

        # allow for a search to work if there are not _items
        if len(self._items) == 0:
            items = self.all()
        else:
            items = self._items

        for i in self._items:
            if callback:
                i = callback(i)

            i = self.execute_callbacks('on_bulk_commit', i)

            this_dict = {}
            this_id = ''
            this_idx = self.idx
            this_type = self.base_obj.__type__
            if isinstance(i, VWBase):
                this_dict = i._create_source_document()
                this_type = i.__type__
                this_id = i.id
                try:
                    this_idx = i.__index__
                except AttributeError:
                    pass

            elif isinstance(i,dict):
                this_dict = i
                this_id = i.get('id')

            else:
                raise TypeError('Elments passed to the collection must be type of "dict" or "VWBase"')

            if not this_id:
                this_id = str(uuid4())

            bulk_docs.append({'_op_type': 'index', '_type': this_type, '_index': this_idx, '_id': this_id, '_source': this_dict})

        return helpers.bulk(self._es,bulk_docs,chunk_size=self.bulk_chunk_size)

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

    def _create_obj(self,doc):
        doc = self.base_obj.execute_class_callbacks('before_auto_create_model', doc)

        src = doc.get('_source')
        src['_set_by_query'] = True
        src['id'] = doc.get('_id')

        obj = self.base_obj(**src)
        return obj.execute_callbacks('after_auto_create_model', obj, **src)

    # python abuse!
    # seriously though we want to act like a list in many cases
    def __getitem__(self,idx):
        return self._create_obj(self.doc_list[idx])

    def __len__(self):
        return len(self.doc_list)

    def results(self):
        return self.es_results
