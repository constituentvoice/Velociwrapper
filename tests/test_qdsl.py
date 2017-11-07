from __future__ import absolute_import
import unittest
from velociwrapper import qdsl


class TestQDSL(unittest.TestCase):
    def test_query(self):
        fragment = qdsl.query(qdsl.match_all())
        self.assertEqual(fragment, {'query': {'match_all': {}}})

    def test_filter_(self):
        fragment = qdsl.filter_(qdsl.term('foo', 'bar'))
        self.assertEqual(fragment, {'filter': {'term': {'foo': {'value': 'bar'}}}})

    def test_match(self):
        fragment = qdsl.match('foo', 'bar')
        self.assertEqual(fragment, {'match': {'foo': {'query': 'bar'}}})

    def test_match_phrase(self):
        fragment = qdsl.match_phrase('foo', 'bar')
        self.assertEqual(fragment, {'match': {'foo': {'query': 'bar'}, 'type': 'phrase'}})

    def test_match_phrase_prefix(self):
        fragment = qdsl.match_phrase_prefix('foo', 'bar')
        self.assertEqual(fragment, {'match': {'foo': {'query': 'bar'}, 'type': 'phrase_prefix'}})

    def test_multi_match(self):
        fragment = qdsl.multi_match('foo', ['bar', 'baz', 'bang'])
        self.assertEqual(fragment, {'multi_match': {'fields': ['bar', 'baz', 'bang'], 'query': 'foo'}})

    def test_bool(self):
        with self.assertRaises(TypeError):
            qdsl.bool_('gobbly-goop')

        fragment = qdsl.bool_(qdsl.must(qdsl.match('foo', 'bar')), qdsl.should(qdsl.match('baz', 'bang')))
        self.assertEqual(fragment, {
            'bool': {
                'must': {
                    'match': {'foo': {'query': 'bar'}}
                },
                'should': {
                    'match': {'baz': {'query': 'bang'}}
                }
            }
        })

    def test_term(self):
        fragment = qdsl.term('foo', 'bar')
        self.assertEqual(fragment, {'term': {'foo': {'value': 'bar'}}})

    def test_terms(self):
        fragment = qdsl.terms('foo', ['bar','baz','bang'])
        self.assertEqual(fragment, {'terms': {'foo': ['bar', 'baz', 'bang']}})

    def test_must(self):
        # test dict input
        fragment = qdsl.must(qdsl.match('foo', 'bar'))
        self.assertEqual(fragment, {'must': {'match': {'foo': {'query': 'bar'}}}})

        # test field / value input (term)
        self.assertEqual(qdsl.must('foo', 'bar'), {'must': {'term': {'foo': {'value': 'bar'}}}})

    def test_must_not(self):
        # test dict input
        fragment = qdsl.must_not(qdsl.match('foo', 'bar'))
        self.assertEqual(fragment, {'must_not': {'match': {'foo': {'query': 'bar'}}}})

        # test field / value input (term)
        self.assertEqual(qdsl.must_not('foo', 'bar'), {'must_not': {'term': {'foo': {'value': 'bar'}}}})

    def test_should(self):
        # test dict input
        fragment = qdsl.should(qdsl.match('foo', 'bar'))
        self.assertEqual(fragment, {'should': {'match': {'foo': {'query': 'bar'}}}})

        # test field / value input (term)
        self.assertEqual(qdsl.should('foo', 'bar'), {'should': {'term': {'foo': {'value': 'bar'}}}})

    def test_boosting(self):
        # single dict
        boost_dict = qdsl.negative('foo', 'bar')
        fragment = qdsl.boosting(boost_dict)
        self.assertEqual(fragment, {'boosting': {'negative': {'term': {'foo': {'value': 'bar'}}}}})

        # multiple dict and key words
        boost_dict2 = qdsl.positive('baz','bang')
        fragment2 = qdsl.boosting(boost_dict, boost_dict2, negative_boost=0.2)

        self.assertEqual(fragment2, {
            'boosting': {
                'negative': {'term': {'foo': {'value': 'bar'}}},
                'positive': {'term': {'baz': {'value': 'bang'}}},
                'negative_boost': 0.2
            }})

    def test_positive(self):
        self.assertEqual(qdsl.positive('baz', 'bang'), {'positive': {'term': {'baz': {'value': 'bang'}}}})

    def test_negative(self):
        self.assertEqual(qdsl.negative('foo', 'bar'), {'negative': {'term': {'foo': {'value': 'bar'}}}})

    def test_common(self):
        self.assertEqual(
            qdsl.common('foo',
                        'this is a phrase with some common and uncommon words',
                        cutoff_frequency=000.1),
                        {
                            'common': {
                                'foo': {
                                    'query': 'this is a phrase with some common and uncommon words',
                                    'cutoff_frequency': 000.1
                                }
                            }
                        })

    def test_constant_score(self):
        fragment = qdsl.constant_score(qdsl.filter_(qdsl.term('foo', 'bar')))
        self.assertEqual(fragment, {'constant_score': {'filter': {'term': {'foo': {'value': 'bar'}}}}})

    def test_function_score(self):
        fragment = qdsl.function_score(
            qdsl.query(qdsl.match_all()),
            functions={
                'filter': qdsl.term('foo', 'bar'),
                'weight': 42
            },
            max_boost=42,
            score_mode="max",
            boost_mode="multiply",
            min_score=42
        )
        self.assertEqual(fragment, {
            'function_score': {
                'query': {'match_all': {}},
                'functions': {
                    'filter': {'term': {'foo': {'value': 'bar'}}},
                    'weight': 42
                },
                'max_boost': 42,
                'score_mode': 'max',
                'boost_mode': 'multiply',
                'min_score': 42
            }
        })

    def test_fuzzy(self):
        self.assertEqual(qdsl.fuzzy('foo', 'bar'), {
            'fuzzy': {'foo':'bar'}
        })

    def test_ids(self):
        output = {'ids': {'values': ['foo', 'bar', 'baz'], 'type': 'vwtestmodel'}}

        # test passing a list
        self.assertEqual(
            qdsl.ids(['foo', 'bar', 'baz'], type='vwtestmodel'),
            output
        )

        # test passing as params
        self.assertEqual(
            qdsl.ids('foo', 'bar', 'baz', type='vwtestmodel'),
            output
        )

        # test a single value
        self.assertEqual(
            qdsl.ids('foo', type='vwtestmodel'),
            {'ids': {'values': ['foo'], 'type': 'vwtestmodel'}}
        )

    def test_query_term(self):
        self.assertEqual(
            qdsl.query_term('foo', 'bar'),
            {'query': {'term': {'foo': {'value': 'bar'}}}}
        )

    def test_indicies(self):
        pass

    def test_match_all(self):
        pass

    def test_more_like_this(self):
        pass

    def test_nested(self):
        pass

    def test_prefix(self):
        pass

    def test_query_string(self):
        pass

    def test_simple_query_string(self):
        pass

    def test_range_(self):
        pass

    def test_regexp(self):
        pass

    def test_span_term(self):
        pass

    def test_span_first(self):
        pass

    def test_span_multi(self):
        pass

    def test_span_near(self):
        pass

    def test_span_not(self):
        pass

    def test_span_or(self):
        pass

    def test_wildcard(self):
        pass

    def test_exists(self):
        pass

    def test_geo_bounding_box(self):
        pass

    def test_geo_distance(self):
        pass

    def test_geo_range(self):
        pass

    def test_geo_polygon(self):
        pass

    def test_geo_shape(self):
        pass

    def test_geohash_cell(self):
        pass

    def test_has_child(self):
        pass

    def test_has_parent(self):
        pass

    def test_missing(self):
        pass

    def test_script(self):
        pass

    def test_type_(self):
        pass

    def test_highlight(self):
        pass
