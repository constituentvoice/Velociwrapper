from __future__ import absolute_import
import os
import json
from unittest import TestCase, SkipTest
from velociwrapper import VWBase, VWCollection, VWConnection, VWConnectionError
from velociwrapper.es_types import Array, Text, Keyword, Integer
from velociwrapper.mapper import Mapper, MapperError
from elasticsearch import RequestError

class TestModel(VWBase):
    __index__ = 'vwtest'
    __type__ = 'vwtestmodel'

    foo = Text()
    bar = Keyword()
    baz = Array(es_properties={'type', 'text'})
    bang = Integer()


class TestCollection(VWCollection):
    __model__ = TestModel


class VWTestSetup(TestCase):
    connection = None

    @classmethod
    def setUpClass(cls):

        # see if we have already connected
        try:
            cls.connection = VWConnection.get_connection()
        except VWConnectionError:

            if os.environ.get('VW_DSN'):
                try:
                    connection_params = json.loads(os.environ.get('VW_CONNECTION_PARAMS'))
                except (TypeError, ValueError):
                    connection_params = {}

                cls.connection = VWConnection.connect(os.environ.get('VW_DSN'), set_default=True, **connection_params)

        if cls.connection:
            try:
                mapper = Mapper()
                mapper.create_indices(index='vwtest')
            except RequestError:
                pass  # should be because it's already created

            tm = TestModel(foo='foo',bar='bar', baz=['tabby', 'tux', 'tortoise shell'], bang=10)
            tm.commit()

    @classmethod
    def tearDownRequest(cls):
        mapper = Mapper()
        try:
            esc = mapper.get_es_client()
            esc.delete(index='vwtest')
            cls.connection = None
        except MapperError:
            pass

    @classmethod
    def requires_connection(cls, func):
        def check_connection():
            if not cls.connection:
                raise SkipTest('No Elasticsearch connection defined. Set VW_DSN environment variable to test.')

            func()
        return check_connection()

