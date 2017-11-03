from __future__ import absolute_import, unicode_literals
from six import iteritems, string_types
import velociwrapper.config
from elasticsearch import client, helpers
from elasticsearch.exceptions import RequestError
import velociwrapper.base
from .util import all_subclasses
from .connection import VWConnection
import inspect
from .es_types import ESType

__all__ = ['MapperError', 'MapperMergeError', 'Mapper']


class MapperError(Exception):
    pass


class MapperMergeError(MapperError):
    pass


# tools for creating or reindexing elasticsearch mapping
class Mapper(object):

    @staticmethod
    def get_es_client(connection=None):
        if not connection:
            connection = VWConnection.get_connection()

        return client.IndicesClient(connection)

    # Retrieves the mapping as defined by the server
    def get_server_mapping(self, index=None, connection=None):
        es_client = self.get_es_client(connection)
        indexes = []
        if isinstance(index, list):
            indexes = index
        # if the model argument is a VWBase object
        elif isinstance(index, velociwrapper.base.VWBase):
            try:
                indexes.append(index.__index__)
            except AttributeError:
                pass
        elif isinstance(index, string_types):
            indexes.append(index)
        elif index:
            raise TypeError('"index" argument must be a string or a list')

        if not indexes:
            indexes.append(velociwrapper.config.default_index)

        return es_client.get_mapping(index=indexes)

    # Retrieves what the map should be according to the defined models
    def get_index_map(self, index=None):
        # recursively find all the subclasses of base

        # options
        """
        * index = "string"
            only map the index defined by "string"

        * index = ['index1','index2' ...]
            map the indexes defined by entries in list

        """

        subclasses = []
        self.get_subclasses(velociwrapper.base.VWBase, subclasses)

        indexes = {}

        index_list = []
        if index:
            if isinstance(index, string_types):
                index_list.append(index)

            elif isinstance(index, list):
                index_list.extend(index)
            else:
                raise TypeError('"index" argument must be a string or list')

        for sc in subclasses:

            try:
                idx = sc.__index__
            except AttributeError:
                idx = velociwrapper.config.default_index

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
                    if isinstance(v, ESType):
                        sc_body[sc.__type__]['properties'][k] = v.prop_dict()
                except AttributeError:
                    pass

            # overwrite with custom-mapping if applicable
            try:
                for k, v in iteritems(sc.__custom_mapping__):
                    sc_body[sc.__type__][k].update(v)
            except AttributeError:
                pass

            indexes[idx]['mappings'].update(sc_body)

        return indexes

    def create_indices(self, suffix=None, connection=None, **kwargs):
        es_client = self.get_es_client(connection)

        indexes = self.get_index_map(**kwargs)

        for k, v in iteritems(indexes):
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
        for k, v in iteritems(aliasd):
            index = k
            break

        if index == alias:
            return None

        return index

    def reindex(self, idx, newindex, alias_name=None, remap_alias=None, connection=None, **kwargs):
        es_client = self.get_es_client(connection)

        # are we an alias or an actual index?
        index = idx
        alias = None
        alias_exists = False
        if es_client.exists_alias(name=alias_name):
            alias = idx
            idx = self.get_index_for_alias(idx, connection)
            if remap_alias:
                alias_exists = True

        if alias_name:
            if es_client.exists_alias(name=alias_name):
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

    @staticmethod
    def get_subclasses(cls, subs):
        subs.extend(all_subclasses(cls))

    @classmethod
    def describe(cls, model):
        body = {}
        for k, v in iteritems(model.__dict__):
            try:
                if isinstance(v, ESType):
                    body[k] = v.prop_dict()
            except AttributeError:
                pass

            if not body.get(k):
                body[k] = {"type": type(v).__name__}

        return body

    def update_type_mapping(self, doc_type, alias, connection=None):
        local_map = self.get_index_map(alias)
        es_client = self.get_es_client(connection)

        if doc_type not in local_map:
            raise MapperError('Type does not exist in this index/alias!')

        try:
            es_client.put_mapping(doc_type, {doc_type: local_map.get(doc_type)}, index=alias)
        except RequestError as e:
            if 'MergeMappingException' in e.error:
                raise MapperMergeError
            else:
                raise MapperError('Mapper Error: Elasticsearch responded {}'.format(e.error))
