from . import config
from elasticsearch import Elasticsearch, client, helpers
from .es_types import *
from .base import VWBase
from .util import all_subclasses
from .connection import VWConnection
import inspect


class MapperError(Exception):
    pass


# tools for creating or reindexing elasticsearch mapping
class Mapper(object):
    def get_es_client(self, connection=None):
        if not connection:
            connection = VWConnection.get_connection()

        return client.IndicesClient(connection)

    # Retrieves the mapping as defined by the server
    def get_server_mapping(self, connection=None, **kwargs):
        es_client = self.get_es_client(connection)
        indexes = []
        if isinstance(kwargs.get('index'), list):
            indexes = kwargs.get('index')
        elif kwargs.get('index'):
            indexes.append(kwargs.get('index'))

        # if the model arguent is a VWBase object
        if isinstance(kwargs.get('index'), VWBase):
            try:
                indexes.append(kwargs.get('index').__index__)
            except AttributeError:
                pass

        if not indexes:
            indexes.append(config.default_index)

        return es_client.get_mapping(index=indexes)

    # Retrieves what the map should be according to the defined models
    def get_index_map(self, **kwargs):
        # recursively find all the subclasses of base

        # options
        """
        * index = "string"
            only map the index defined by "string"

        * index = ['index1','index2' ...]
            map the indexes defined by entries in list

        """

        subclasses = []
        self.get_subclasses(VWBase, subclasses)

        indexes = {}

        index_list = []
        if kwargs.get('index'):
            if isinstance(kwargs.get('index'), str):
                index_list.append(kwargs.get('index'))

            elif isinstance(kwargs.get('index'), list):
                index_list.extend(kwargs.get('index'))
            else:
                raise TypeError('"index" argument must be a string or list')

        for sc in subclasses:

            try:
                idx = sc.__index__
            except AttributeError:
                idx = config.default_index

            if len(index_list) > 0 and idx not in index_list:
                continue

            if idx not in indexes:
                indexes[idx] = {"mappings": {}}

            try:
                # create the basic body
                sc_body = {sc.__type__: {"properties": {}}}
            except AttributeError:
                # fails when no __type__ is found. Likely a subclass
                # to add other features. We will skip mapping
                continue

            for k, v in inspect.getmembers(sc):
                try:
                    if v.__metaclass__ == ESType:
                        sc_body[sc.__type__]['properties'][k] = v.prop_dict()
                except AttributeError:
                    pass

            indexes[idx]['mappings'].update(sc_body)

        return indexes

    def create_indices(self, connection=None, **kwargs):
        es_client = self.get_es_client(connection)

        suffix = kwargs.get('suffix')
        indexes = self.get_index_map(**kwargs)

        for k, v in indexes.iteritems():
            if suffix:
                idx = k + suffix
            else:
                idx = k

            es_client.create(index=idx, body=v)

            if suffix:
                es_client.put_alias(index=idx, name=k)

    def get_index_for_alias(self, alias, connection=None):
        es_client = self.get_es_client(connection)

        aliasd = es_client.get_aliases(index=alias)
        index = ''
        for k, v in aliasd.iteritems():
            index = k
            break

        if index == alias:
            return None

        return index

    def reindex(self, idx, newindex, alias_name=None, remap_alias=None, connection=None, **kwargs):
        es_client = self.get_es_client(connection)

        # are we an alias or an actual index?
        index = idx;
        alias = None
        alias_exists = False

        if es_client.exists_alias(idx):
            alias = idx
            index = self.get_index_for_alias(idx)
            alias_exists = True

        if alias_name:
            if es_client.exists_alias(alias_name):
                if remap_alias:
                    alias_exists = True
                else:
                    raise MapperError(
                        '%s already exists as an alias. If you wish to delete the old alias pass remap_alias=True' % alias_name)

            alias = alias_name

        # does the new index exist?
        if not es_client.exists(newindex):
            # if new doesn't exist then create the mapping
            # as a copy of the old one. The idea being that the mapping
            # was changed
            index_mapping = self.get_index_map(
                index=idx)  # using "idx" intentionally because models will be defined as alias

            # have to use the index name as the key to the dict even though only one is returned.
            # .create() only takes the mapping
            es_client.create(index=newindex, body=index_mapping.get(idx))

        # map our documents
        helpers.reindex(es_client, index, newindex, **kwargs)

        if alias and (remap_alias or alias_name):
            if alias_exists:
                es_client.delete_alias(name=alias, index=index)

            es_client.put_alias(name=alias, index=newindex)

    def get_subclasses(self, cls, subs):
        subs.extend(all_subclasses(cls))

    def describe(self, cls):
        body = {}
        for k, v in cls.__dict__.iteritems():
            try:
                if v.__metaclass__ == ESType:
                    body[k] = v.prop_dict()
            except AttributeError:
                pass

            if not body.get(k):
                body[k] = {"type": type(v).__name__}

        return body
