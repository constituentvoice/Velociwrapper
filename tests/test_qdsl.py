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
        output = {'match': {'foo': {'query': 'bar'}}}

        # field / value
        fragment = qdsl.match('foo', 'bar')
        self.assertEqual(fragment, output)

        # dict input
        self.assertEqual(qdsl.match({'foo': {'query': 'bar'}}), output)

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
        fragment = qdsl.terms('foo', ['bar', 'baz', 'bang'])
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
        boost_dict2 = qdsl.positive('baz', 'bang')
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
                        cutoff_frequency=000.1), {
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
            'fuzzy': {'foo': 'bar'}
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

    def test_indices(self):
        #  test as dict
        params = {
            'indices': ['vwindex1', 'vwindex2'],
            'query': {"term": {"animal": "cat"}},
            'no_match_query': {"term": {"animal": "dog"}}
        }
        self.assertEqual(qdsl.indices(params), {'indices': params})

        #  test as list
        fragment = qdsl.indices(
            ['vwindex1', 'vwindex2'],
            {'query': {'term': {'animal': 'cat'}}},
            {'no_match_query': {'term': {'animal': 'dog'}}}
        )
        self.assertEqual(fragment, {'indices': params})

    def test_match_all(self):
        self.assertEqual(qdsl.match_all(), {'match_all': {}})

    def test_more_like_this(self):
        # test with dict parameter
        params = {
            'fields': ['title', 'description'],
            'like': 'Once upon a time',
            'min_term_freq': 1,
            'max_query_terms': 12
        }
        self.assertEqual(qdsl.more_like_this(params), {'more_like_this': params})

        # test with keywords
        self.assertEqual(qdsl.more_like_this(
            fields=['title', 'description'],
            like='Once upon a time',
            min_term_freq=1,
            max_query_terms=12
        ), {'more_like_this': params})

    def test_nested(self):
        params = {
            'path': 'animals',
            'score_mode': 'avg',
            'query': qdsl.bool_(
                qdsl.must(
                    qdsl.match('animal', 'cat'),
                    qdsl.range_('pet_count', gt=5)
                ))
        }
        # dict
        self.assertEqual(qdsl.nested(params), {'nested': params})

        # keywords
        self.assertEqual(qdsl.nested(
            path='animals',
            score_mode='avg',
            query=qdsl.bool_(qdsl.must(qdsl.match('animal', 'cat'), qdsl.range_('pet_count', gt=5)))
        ), {'nested': params})

    def test_prefix(self):
        # parameters
        self.assertEqual(qdsl.prefix('breed', 'tort'), {'prefix': {'breed': {'value': 'tort'}}})

        # dict
        self.assertEqual(qdsl.prefix({'breed': {'value': 'tort'}}), {'prefix': {'breed': {'value': 'tort'}}})

    def test_query_string(self):
        self.assertEqual(
            qdsl.query_string('this AND that OR thus', default_field='content'),
            {'query_string': {'query': 'this AND that OR thus', 'default_field': 'content'}}
        )

    def test_simple_query_string(self):
        self.assertEqual(
            qdsl.simple_query_string('(this + that) | thus',
                                     fields=['title', 'description'],
                                     default_operator='and'
                                     ),
            {'simple_query_string': {
                'query': '(this + that) | thus',
                'fields': ['title', 'description'],
                'default_operator': 'and'
            }}
        )

    def test_range_(self):
        # keywords
        self.assertEqual(
            qdsl.range_('some_field', gt=5),
            {'range': {'some_field': {'gt': 5}}}
        )

        # dict
        self.assertEqual(qdsl.range_({'some_field': {'gt': 5}}), {'range': {'some_field': {'gt': 5}}})

    def test_regexp(self):
        param = {
            'animals.breed': {
                'value': 'tortoise (shell|cat)',
                'boost': 1.2
            }
        }

        # dict
        self.assertEqual(qdsl.regexp(param), {'regexp': param})

        # field + regex + keywords
        self.assertEqual(
            qdsl.regexp('animals.breed', 'tortoise (shell|cat)', boost=1.2),
            {'regexp': param}
        )

    def test_span_term(self):
        params = {'animal': {'value': 'cat', 'boost': 2.0}}

        # dict
        self.assertEqual(qdsl.span_term(params), {'span_term': params})

        # field, value and keywords
        self.assertEqual(qdsl.span_term('animal', 'cat', boost=2.0), {'span_term': params})

    def test_span_first(self):

        # test exception
        with self.assertRaises(TypeError):
            qdsl.span_first('gobbly-goop')

        output = {
            'span_first': {
                'end': 3,
                'match': {
                    'span_term': {
                        'animal': {'value': 'cat'}
                    }
                }
            }
        }

        # test with qdsl match parameters
        fragment = qdsl.span_first(qdsl.match(qdsl.span_term('animal', 'cat')), end=3)
        self.assertEqual(fragment, output)

        # test with qdsl dict (omitting match)
        fragment2 = qdsl.span_first(qdsl.span_term('animal', 'cat'), end=3)
        self.assertEqual(fragment2, output)

        # test with field names in kwargs
        fragment3 = qdsl.span_first(animal='cat', end=3)
        self.assertEqual(fragment3, output)

    def test_span_multi(self):
        with self.assertRaises(TypeError):
            qdsl.span_multi('gobbly-goop')

        self.assertEqual(
            qdsl.span_multi(
                qdsl.prefix('breed', 'tort', boost=1.0)
            ),
            {'span_multi': {
                'match': {
                    'prefix': {'breed': {'value': 'tort', 'boost': 1.0}}
                }
            }}
        )

    def test_span_near(self):
        with self.assertRaises(TypeError):
            qdsl.span_near('gobbly-goop')

        params = [qdsl.span_term('animal', 'cat'), qdsl.span_term('breed', 'tortoise shell')]
        output = {'span_near': {'clauses': params, 'slop': 12, 'in_order': False}}

        # pass list of dicts
        self.assertEqual(qdsl.span_near(params, slop=12, in_order=False), output)

        # pass params
        self.assertEqual(qdsl.span_near(*params, slop=12, in_order=False), output)

    def test_span_not(self):
        with self.assertRaises(TypeError):
            qdsl.span_not(include='gobbly-goop')

        with self.assertRaises(TypeError):
            qdsl.span_not(exclude='gobbly-goop')

        params = [qdsl.span_term('breed', 'tabby'), qdsl.span_term('breed', 'tortoise shell')]
        self.assertEqual(qdsl.span_not(
            include=qdsl.span_term('animal', 'cat'),
            exclude=qdsl.span_near(params, slop=2, in_order=True)
            ),
            {
                'span_not': {
                    'include': {
                        'span_term': {'animal': {'value': 'cat'}}
                    },
                    'exclude': {
                        'span_near': {'clauses': params, 'slop': 2, 'in_order': True}
                    }
                }
            }
        )

    def test_span_or(self):
        with self.assertRaises(TypeError):
            qdsl.span_or('gobbly-goop')

        params = [qdsl.span_term('animal', 'cat'), qdsl.span_term('breed', 'tortoise shell')]
        output = {'span_or': {'clauses': params}}

        # pass list of dicts
        self.assertEqual(qdsl.span_or(params), output)

        # pass params
        self.assertEqual(qdsl.span_or(*params), output)

    def test_span_containing(self):

        with self.assertRaises(TypeError):
            qdsl.span_containing(little='gobbly-goop')

        with self.assertRaises(TypeError):
            qdsl.span_containing(big='gobbly-goop')

        params = [qdsl.span_term('breed', 'tabby'), qdsl.span_term('breed', 'tortoise shell')]
        self.assertEqual(qdsl.span_containing(
            little=qdsl.span_term('animal', 'cat'),
            big=qdsl.span_near(params, slop=2, in_order=True)
            ),
            {
                'span_containing': {
                    'little': {
                        'span_term': {'animal': {'value': 'cat'}}
                    },
                    'big': {
                        'span_near': {'clauses': params, 'slop': 2, 'in_order': True}
                    }
                }
            }
        )

    def test_span_within(self):

        with self.assertRaises(TypeError):
            qdsl.span_within(little='gobbly-goop')

        with self.assertRaises(TypeError):
            qdsl.span_within(big='gobbly-goop')

        params = [qdsl.span_term('breed', 'tabby'), qdsl.span_term('breed', 'tortoise shell')]
        self.assertEqual(qdsl.span_within(
            little=qdsl.span_term('animal', 'cat'),
            big=qdsl.span_near(params, slop=2, in_order=True)
            ),
            {
                'span_within': {
                    'little': {
                        'span_term': {'animal': {'value': 'cat'}}
                    },
                    'big': {
                        'span_near': {'clauses': params, 'slop': 2, 'in_order': True}
                    }
                }
            }
        )

    def test_field_masking_span(self):
        with self.assertRaises(TypeError):
            qdsl.field_masking_span('gobbly-goop')

        params = qdsl.span_term('animal.stems', 'cat')
        expected = {'field_masking_span': {'query': params, 'field': 'animal'}}

        # as dict with query
        self.assertEqual(
            qdsl.field_masking_span({'query': params, 'field': 'animal'}),
            expected
        )

        # as dict without query
        self.assertEqual(
            qdsl.field_masking_span(params, field='animal'),
            expected
        )

    def test_wildcard(self):
        fragment = qdsl.wildcard('animal', 'cat', boost=2)
        expected = {
            'wildcard': {
                'animal': {
                    'value': 'cat',
                    'boost': 2
                }
            }
        }
        self.assertEqual(fragment, expected)

    def test_exists(self):
        self.assertEqual(qdsl.exists('animal'), {'exists': {'field': 'animal'}})

    def test_geo_bounding_box(self):
        params = {
            'pin.location': {
                'top_left': {
                    'lat': 40.73,
                    'lon': -74.1
                },
                'bottom_right': {
                    'lat': 40.01,
                    'lon': -74.12
                }
            }
        }
        expected = {'geo_bounding_box': params}

        # dict
        self.assertEqual(qdsl.geo_bounding_box(params), expected)

        # field and keywords
        self.assertEqual(qdsl.geo_bounding_box(
            'pin.location',
            top_left={'lat': 40.73, 'lon': -74.1},
            bottom_right={'lat': 40.01, 'lon': -74.12}
            ),
            expected
        )

    def test_geo_distance(self):
        with self.assertRaises(TypeError):
            qdsl.geo_distance('gobbly-goop')

        expected = {
            "geo_distance": {
                "distance": "200km",
                "pin.location": {
                    "lat": 40,
                    "lon": -70
                }
            }
        }

        # dict
        self.assertEqual(
            qdsl.geo_distance({
                'distance': "200km",
                "pin.location": {
                    "lat": 40,
                    "lon": -70
                }
            }), expected
        )

        # fields
        self.assertEqual(
            qdsl.geo_distance("pin.location", {'lat': 40, 'lon': -70}, "200km"),
            expected
        )

    def test_geo_range(self):
        with self.assertRaises(TypeError):
            qdsl.geo_range('gobbly-goop')

        params = {
            "from": "200km",
            "to": "400km",
            "pin.location": {
                "lat": 40,
                "lon": -70
            }
        }

        expected = {'geo_distance_range': params}

        # dict
        self.assertEqual(qdsl.geo_range(params), expected)

        # fields
        self.assertEqual(
            qdsl.geo_range('pin.location', {'lat': 40, 'lon': -70}, "200km", "400km"),
            expected
        )

    def test_geo_polygon(self):
        with self.assertRaises(TypeError):
            qdsl.geo_polygon('user.location', 'gobbly-goop')

        params = {
            "user.location": {
                "points": [
                    {"lat": 40, "lon": -70},
                    {"lat": 30, "lon": -80},
                    {"lat": 20, "lon": -90}
                ]
            }
        }
        expected = {'geo_polygon': params}

        # dict
        self.assertEqual(qdsl.geo_polygon(params), expected)

        # fields
        self.assertEqual(qdsl.geo_polygon(
            "user.location",
            [{"lat": 40, "lon": -70},
             {"lat": 30, "lon": -80},
             {"lat": 20, "lon": -90}]
            ),
            expected
        )

    def test_geo_shape(self):
        with self.assertRaises(TypeError):
            qdsl.geo_shape('gobbly-goop')

        params = {
            'location': {
                'shape': {
                    'type': 'envelope',
                    'coordinates': [[12.0, 53.0], [14.0, 52.0]]
                },
                'relation': 'within'
            }
        }

        expected = {'geo_shape': params}

        # dict
        self.assertEqual(qdsl.geo_shape(params), expected)

        # fields shape
        self.assertEqual(
            qdsl.geo_shape('location', shape={
                'type': 'envelope',
                'coordinates': [[12.0, 53.0], [14.0, 52.0]]
            }, relation='within'),
            expected
        )

        # fields indexed_shape
        indexed_shape_params = {
            'index': 'shapes',
            'type': 'doc',
            'id': 'shape1',
            'path': 'location'
        }
        expected2 = {'geo_shape': {'location': {'indexed_shape': indexed_shape_params}}}

        self.assertEqual(
            qdsl.geo_shape('location', indexed_shape=indexed_shape_params),
            expected2
        )

    def test_geohash_cell(self):
        self.assertEqual(qdsl.geohash_cell('location', 40, -70), {
            'geohash_cell': {'location': {'lat': 40, 'lon': -70}}})

    def test_has_child(self):
        with self.assertRaises(TypeError):
            qdsl.has_child('gobbly-goop')

        params = {'type': 'animals', 'query': qdsl.term('species', 'cat')}
        expected = {'has_child': params}

        # type and query
        self.assertEqual(qdsl.has_child('animals', query=qdsl.term('species', 'cat')), expected)

        # dict
        self.assertEqual(qdsl.has_child(params), expected)

    def test_has_parent(self):
        with self.assertRaises(TypeError):
            qdsl.has_parent('gobbly-goop')

        params = {'type': 'animals', 'query': qdsl.term('species', 'cat')}
        expected = {'has_parent': params}

        # type and query
        self.assertEqual(qdsl.has_parent('animals', query=qdsl.term('species', 'cat')), expected)

        # dict
        self.assertEqual(qdsl.has_parent(params), expected)

    def test_missing(self):
        self.assertEqual(qdsl.missing('species'), qdsl.must_not(qdsl.exists('species')))

    def test_script(self):
        with self.assertRaises(TypeError):
            qdsl.script('gobbly-goop')

        params = {'source': "doc['num1'].value > 1", 'lang': 'painless'}
        expected = {'script': {'script': params}}

        # dict
        self.assertEqual(qdsl.script(params), expected)

        # source / lang
        self.assertEqual(qdsl.script(params['source'], params['lang']), expected)

    def test_type_(self):
        self.assertEqual(qdsl.type_('animals'), {'type': {'value': 'animals'}})

    def test_highlight(self):
        self.assertEqual(qdsl.highlight(title={}, description={}), {
            'highlight': {'fields': {
                'title': {},
                'description': {}
            }}
        })
