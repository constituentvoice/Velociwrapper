from __future__ import absolute_import
from .setup import VWTestSetup
from velociwrapper.mapper import Mapper, MapperError, MapperMergeError
from velociwrapper.util import VWDialect
from elasticsearch.client import IndicesClient


class TestMapper(VWTestSetup):

    @VWTestSetup.requires_connection
    def test_es_client(self):
        self.assertIsInstance(Mapper().get_es_client(), IndicesClient)

    @VWTestSetup.requires_connection
    def test_server_mapping(self):
        dialect = VWDialect.dialect()
        text_type = {'type': 'text'}
        keyword_type = {'type': 'keyword'}
        if dialect < 5:
            text_type['type'] = 'string'
            keyword_type['type'] = 'string'
            keyword_type['index'] = 'not_analyzed'

    def test_index_map(self):
        pass

    def test_create_indicies(self):
        pass

    def test_get_index_for_alias(self):
        pass

    def test_reindex(self):
        pass

    def test_get_subclasses(self):
        pass

    def test_describe(self):
        pass

    def test_update_type_mapping(self):
        pass
