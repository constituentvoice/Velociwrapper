from __future__ import absolute_import
from six import string_types
import unittest
from datetime import datetime, date
from velociwrapper.es_types import (
    ESType,
    Integer,
    Boolean,
    Long,
    Float,
    DateTime,
    Date,
    IP,
    Text,
    Keyword,
    Array,
    String,
    Double,
    Short,
    Binary,
    GeoPoint,
    TokenCount,
    Percolator,
    Join
)


class TestESTypes(unittest.TestCase):
    def test_is_es_properties_analyzed(self):
        self.assertFalse(ESType.is_es_properties_analyzed({'analyzed': False}))
        self.assertFalse(ESType.is_es_properties_analyzed({'index': 'not_analyzed'}))
        self.assertTrue(ESType.is_es_properties_analyzed({'analyzed': True}))
        self.assertTrue(ESType.is_es_properties_analyzed({'index': 'analyzed'}))

    def test_create(self):
        self.assertIsInstance(ESType.create(1), Integer)
        self.assertIsInstance(ESType.create(True), Boolean)
        self.assertIsInstance(ESType.create(3.1415), Float)
        self.assertIsInstance(ESType.create('2001-12-29T14:00:00.0000Z-0500'), DateTime)
        self.assertIsInstance(ESType.create('2001-12-29T14:00:00.0000'), DateTime)
        self.assertIsInstance(ESType.create('2001-12-29T14:00:00'), DateTime)
        self.assertIsInstance(ESType.create('2001-12-29 14:00:00'), DateTime)
        self.assertIsInstance(ESType.create('2001-12-29'), Date)
        self.assertIsInstance(ESType.create('192.168.2.1'), IP)
        self.assertIsInstance(ESType.create('The quick brown fox'), Text)
        self.assertIsInstance(ESType.create('The quick brown fox', es_properties={'analyzed': False}), Keyword)

    def test_is_analyzed(self):
        self.assertTrue(ESType.is_analyzed('will be analyzed'))
        self.assertFalse(ESType.is_analyzed(Keyword('not analyzed')))
        self.assertFalse(ESType.is_analyzed(1))  # nope

    def test_build_map(self):
        list_params = [Text('foo'), Keyword('bar')]
        expected_dialect_1_2_list = [{'type': 'string'}, {'type': 'string', 'index': 'not_analyzed'}]

        dict_params = {
            'foo': Text('foo'),
            'bar': Keyword('bar')
        }

        expected_dialect_1_2_dict = {
            'foo': {'type': 'string'},
            'bar': {'type': 'string', 'index': 'not_analyzed'}
        }

        self.assertEqual(
            ESType.build_map(
                list_params,
                dialect=5
            ),
            [{'type': 'text'}, {'type': 'keyword'}]
        )

        self.assertEqual(
            ESType.build_map(
                list_params,
                dialect=2
            ),
            expected_dialect_1_2_list
        )

        self.assertEqual(
            ESType.build_map(
                list_params,
                dialect=1
            ),
            expected_dialect_1_2_list
        )

        self.assertEqual(
            ESType.build_map(
                dict_params,
                dialect=5
            ),
            {'foo': {'type': 'text'}, 'bar': {'type': 'keyword'}}
        )

        self.assertEqual(
            ESType.build_map(
                dict_params,
                dialect=2
            ),
            expected_dialect_1_2_dict
        )

        self.assertEqual(
            ESType.build_map(
                dict_params,
                dialect=1
            ),
            expected_dialect_1_2_dict
        )

        self.assertEqual(ESType.build_map(1), {'type': 'integer'})

    def test_analyzed_getter(self):
        self.assertFalse(Keyword('foobar').analyzed)

    def test_array(self):
        test = Array(['foo', 'bar', 'baz'])
        self.assertIsInstance(test, Array)
        self.assertIsInstance(test, list)
        self.assertEqual(test.prop_dict(dialect=5), {'type': 'text'})
        self.assertEqual(test.prop_dict(dialect=1), {'type': 'string'})
        self.assertEqual(test.prop_dict(dialect=2), {'type': 'string'})

        test2 = Array(['foo', 'bar', 'baz'], es_properties={'type': 'keyword'})
        self.assertEqual(test2.prop_dict(dialect=5), {'type': 'keyword'})
        self.assertEqual(test2.prop_dict(dialect=1), {'type': 'string', 'index': 'not_analyzed'})
        self.assertEqual(test2.prop_dict(dialect=2), {'type': 'string', 'index': 'not_analyzed'})

    def test_string(self):
        self.assertIsInstance(String('foo'), String)
        self.assertIsInstance(String('foo'), string_types)
        self.assertEqual(String('foo').prop_dict(), {'type': 'string'})

    def test_text(self):
        self.assertIsInstance(Text('foo'), Text)
        self.assertIsInstance(Text('foo'), string_types)
        self.assertEqual(Text('foo').prop_dict(dialect=5), {'type': 'text'})
        self.assertEqual(Text('foo').prop_dict(dialect=2), {'type': 'string'})
        self.assertEqual(Text('foo').prop_dict(dialect=1), {'type': 'string'})

    def test_keyword(self):
        self.assertIsInstance(Keyword('foo'), Keyword)
        self.assertIsInstance(Keyword('foo'), string_types)
        self.assertEqual(Keyword('foo').prop_dict(dialect=5), {'type': 'keyword'})
        self.assertEqual(Keyword('foo').prop_dict(dialect=2), {'type': 'string', 'index': 'not_analyzed'})
        self.assertEqual(Keyword('foo').prop_dict(dialect=1), {'type': 'string', 'index': 'not_analyzed'})

    def test_integer(self):
        self.assertIsInstance(Integer(1), Integer)
        self.assertIsInstance(Integer(1), int)
        self.assertEqual(Integer(1).prop_dict(), {'type': 'integer'})

    def test_long(self):
        value = Long(123456789101112131415)
        self.assertIsInstance(value, Long)
        self.assertIsInstance(value, long)
        self.assertEqual(value.prop_dict(), {'type': 'long'})

    def test_float(self):
        value = Float(3.1415)
        self.assertIsInstance(value, Float)
        self.assertIsInstance(value, float)
        self.assertEqual(value.prop_dict(), {'type': 'float'})

    def test_double(self):
        value = Double(3.1415)
        self.assertIsInstance(value, Double)
        self.assertIsInstance(value, float)
        self.assertEqual(value.prop_dict(), {'type': 'double'})

    def test_short(self):
        self.assertIsInstance(Short(1), Short)
        self.assertIsInstance(Short(1), int)
        self.assertEqual(Short(1).prop_dict(), {'type': 'short'})

    def test_boolean(self):
        # Booleans can't be true Python bools :(
        self.assertIsInstance(Boolean(True), Boolean)
        self.assertTrue(bool(Boolean(True)))
        self.assertFalse(bool(Boolean(False)))
        self.assertEqual(Boolean(True).prop_dict(), {'type': 'boolean'})

    def test_datetime(self):
        value = DateTime(2001, 12, 29, 14, 0, 0)
        value_str = DateTime('2001-12-29 14:00:00')
        value_obj = DateTime(datetime(2001, 12, 29, 14, 0, 0))
        self.assertIsInstance(value, DateTime)
        self.assertIsInstance(value_str, DateTime)
        self.assertIsInstance(value_obj, DateTime)
        self.assertIsInstance(value, datetime)
        self.assertEqual(value.prop_dict(), {'type': 'date'})

    def test_date(self):
        value = Date(2001, 12, 29)
        value_str = Date('2001-12-29')
        value_obj = Date(date(2001, 12, 29))
        self.assertIsInstance(value, Date)
        self.assertIsInstance(value_str, Date)
        self.assertIsInstance(value_obj, Date)
        self.assertIsInstance(value, date)
        self.assertEqual(value.prop_dict(), {'type': 'date'})

    def test_ip(self):
        value = IP('192.168.1.1')
        self.assertIsInstance(value, IP)
        self.assertEqual(value.prop_dict(), {'type': 'ip'})

    def test_binary(self):
        self.assertIsInstance(Binary('gobbbly goop as string'), Binary)
        self.assertEqual(Binary('gobbly goop as string').prop_dict(), {'type': 'binary'})

    def test_geo_point(self):
        self.assertIsInstance(GeoPoint([40, -70]), GeoPoint)
        self.assertIsInstance(GeoPoint([40, -70]), list)
        self.assertEqual(GeoPoint([40,-70]).prop_dict(), {'type': 'geo_point'})

    def test_token_count(self):
        self.assertIsInstance(TokenCount(), TokenCount)
        self.assertEqual(TokenCount().prop_dict(), {'type': 'token_count'})

    def test_percolator(self):
        self.assertIsInstance(Percolator(), Percolator)
        self.assertEqual(Percolator().prop_dict(), {'type': 'percolator'})

    def test_join(self):
        self.assertIsInstance(Join(), Join)
        self.assertEqual(Join().prop_dict(), {'type': 'join'})
