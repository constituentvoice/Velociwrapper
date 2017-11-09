from __future__ import absolute_import
from .setup import VWTestSetup
from velociwrapper.mapper import Mapper
from elasticsearch.client import IndicesClient


class TestMapper(VWTestSetup):
    @classmethod
    def setUpClass(cls):
        super(TestMapper, cls).setUpClass()

    @classmethod
    def tearDownClass(cls):
        super(TestMapper, cls).tearDownClass()

    def test_es_client(self):
        self.skip_unless_connected()
        self.assertIsInstance(Mapper().get_es_client(), IndicesClient)

    def test_server_mapping(self):
        self.skip_unless_connected()
        self.assertIsInstance(Mapper().get_server_mapping(index='vwtest'), dict)

    def test_index_map(self):
        index_map = Mapper().get_index_map()
        self.assertTrue('vwtest' in index_map)

    def test_create_indicies(self):
        self.skip_unless_connected()
        Mapper().create_indices(index='vw_test_cat')
        server_map = Mapper().get_server_mapping(index='vw_test_cat')
        self.assertTrue('vw_test_cat' in server_map)

    def test_get_index_for_alias(self):
        self.skip_unless_connected()
        index = Mapper().get_index_for_alias('vwtest')
        self.assertEqual(index, 'vwtest_v1')

    def test_reindex(self):
        pass

    def test_get_subclasses(self):
        pass

    def test_describe(self):
        pass

    def test_update_type_mapping(self):
        pass
