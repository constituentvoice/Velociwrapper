from __future__ import absolute_import, division, print_function, unicode_literals
from six import string_types, iteritems, with_metaclass
import socket
from datetime import date, datetime
import re

__all__ = ['ESType', 'Array', 'String', 'Text', 'Number', 'Keyword', 'Integer', 'Long', 'Float', 'Double', 'Short',
           'Boolean', 'DateTime', 'Date', 'IP', 'Binary', 'GeoPoint', 'TokenCount', 'Percolator', 'Join']

# The metaclass is defined to allow additional elasticsearch keywords to be passed
# to normal objects
class VWMeta(type):
    def __call__(cls, *args, **kwargs):
        es_args = {}
        if isinstance(kwargs.get('es_properties'), dict):
            es_args = kwargs['es_properties']

        try:
            del kwargs['es_properties']
        except KeyError:
            pass

        inst = super(VWMeta, cls).__call__(*args, **kwargs)
        inst.__es_properties__ = es_args

        return inst


class ESType(with_metaclass(VWMeta, object)):
    _analyzed = None
    __es_properties__ = {}  # should always be overridden

    @classmethod
    def create(cls, value, es_properties=None):
        # see if we already are an ESType
        if isinstance(value, cls):
            return value

        if isinstance(value, string_types):
            # strings could be a lot of things
            # try to see if it's a date

            test_date = value.strip()
            test_date = re.sub("(?:Z|\s*[+\-]\d\d:?\d\d)$", '', test_date)

            try:
                test_date = datetime.strptime(test_date, '%Y-%m-%d %H:%M:%S')
                return DateTime(test_date, es_properties=es_properties)
            except ValueError:
                try:
                    test_date = datetime.strptime(test_date, '%Y-%m-%dT%H:%M:%S')
                    return DateTime(test_date, es_properties=es_properties)
                except ValueError:
                    try:
                        test_date = datetime.strptime(test_date, '%Y-%m-%dT%H:%M:%S.%f')
                        return DateTime(test_date, es_properties=es_properties)
                    except ValueError:
                        try:
                            test_date = datetime.strptime(test_date, '%Y-%m-%d')
                            return Date(test_date.date(), es_properties=es_properties)
                        except ValueError:
                            pass

            # check for an IP address
            try:
                socket.inet_aton(value)
                return IP(value, es_properties=es_properties)
            except socket.error:
                pass

            # check for numeric type
            try:
                if value.isnumeric():
                    return Float(value, es_properties=es_properties)
                else:
                    return String(value, es_properties=es_properties)
            except AttributeError:
                # TODO this could be a byte string
                # which could cause problems, we may need to try to detect
                return String(value, es_properties=es_properties)

        # not a string, start checking for other types
        if isinstance(value, int):
            return Integer(value, es_properties=es_properties)

        if isinstance(value, bool):
            return Boolean(value, es_properties=es_properties)

        if isinstance(value, long):
            return Long(value, es_properties=es_properties)

        if isinstance(value, float):
            return Float(value, es_properties=es_properties)

        if isinstance(value, datetime):
            return DateTime(value, es_properties=es_properties)

        if isinstance(value, date):
            return Date(value, es_properties=es_properties)

        # else just return the value itself
        return value

    # this is to determine if the field should be analyzed
    # based on type and settings. Used a lot to determine whether to use
    # term filters or matches
    # works with estypes and non-estypes
    @classmethod
    def is_analyzed(cls, value):

        if isinstance(value, cls):
            if isinstance(value, String) or isinstance(value, Array):
                analyzed = value.analyzed
            else:
                analyzed = False

        elif isinstance(value, list):
            try:
                analyzed = next(True for item in value if isinstance(item, string_types))
            except StopIteration:
                analyzed = False

        else:
            analyzed = False

        return analyzed

    @classmethod
    def build_map(cls, d):
        if isinstance(d, list):
            return [cls.build_map(i) for i in d]
        elif isinstance(d, dict):
            output = {}
            for (k, v) in iteritems(d):
                if isinstance(v, dict):
                    output[k] = cls.build_map(v)
                else:
                    v = cls.create(v)
                    output[k] = v.prop_dict()

            return output
        else:
            d = cls.create(d)
            return d.prop_dict()

    @property
    def analyzed(self):
        if self._analyzed is None:
            if self.__class__ == Keyword:
                return False
            else:
                return True
        else:
            return self._analyzed

    @analyzed.setter
    def analyzed(self, analyzed_value):
        self._analyzed = analyzed_value

    def prop_dict(self):
        try:
            es_type = self.__class__.type_
        except AttributeError:
            es_type = self.__class__.__name__.lower()

        _output = {"type": es_type}
        _output.update(self.__es_properties__)

        if self.analyzed:
            _output['index'] = True
        elif self.analyzed is False:
            _output['index'] = False

        return _output


class Array(list, ESType):
    type_ = 'string'  # default


class String(str, ESType):
    type_ = 'string'
    _analyzed = True,
    __es_properties__ = {}


class Text(String):
    type_ = 'text',
    _analyzed = True
    __es_properties__ = {}


class Keyword(str, ESType):
    _analyzed = False
    __es_properties__ = {}


class Number(ESType):
    __es_properties__ = dict(
        precision_step=16,
        ignore_malformed=False,
        coerce=True
    )


class Integer(int, Number):
    type_ = 'integer'
    __es_properties__ = dict(
        Number.__es_properties__,
    )


class Long(long, Number):
    type_ = 'long'
    __es_properties__ = dict(
        Number.__es_properties__
    )


class Float(float, Number):
    type_ = 'float'
    __es_properties__ = dict(
        Number.__es_properties__
    )


class Double(float, Number):
    type_ = 'double'
    __es_properties__ = dict(
        Number.__es_properties__
    )


class Short(int, Number):
    type_ = 'short'
    __es_properties__ = dict(Number.__es_properties__)


class Boolean(ESType):
    type_ = 'boolean'

    def __init__(self, value):
        self.value = bool(value)

    def __nonzero__(self):
        return self.value

    def __repr__(self):
        return str(self.value)


class DateTime(datetime, ESType):
    type_ = 'date'
    __es_properties__ = {}

    def __new__(cls, *args, **kwargs):
        try:
            args[0]
        except IndexError:
            args = list(args)
            args[0] = datetime.now()

        if isinstance(args[0], datetime):
            a = args[0]
            args = [a.year, a.month, a.day, a.hour, a.minute, a.second, a.microsecond, a.tzinfo]

        return super(DateTime, cls).__new__(cls, *args, **kwargs)

    def date(self):
        value = super(DateTime, self).date()
        return Date(value)


class Date(date, ESType):
    type_ = 'date'
    __es_properties__ = {}

    def __new__(cls, *args, **kwargs):
        try:
            args[0]
        except IndexError:
            args = list(args)
            args[0] = date.today()

        if isinstance(args[0], date):
            a = args[0]
            args = [a.year, a.month, a.day]

        return super(Date, cls).__new__(cls, *args, **kwargs)


# TODO eventually this should subclass the ipaddress module in Python 3.3+
class IP(str, ESType):
    type_ = 'ip'
    __es_properties__ = {}

    def __new__(cls, value, es_properties=None):
        try:
            socket.inet_aton(value)
        except socket.error:
            raise ValueError('Not a valid IP address')

        super(IP, cls).__new__(value, es_properties=es_properties)


class Binary(object):
    type_ = 'binary'
    __es_properties__ = {}


class GeoPoint(ESType):
    type_ = 'geo_point'
    __es_properties__ = {}


class TokenCount(ESType):
    type_ = 'token_count'
    __es_properties__ = {}


class Percolator(ESType):
    type_ = 'percolator'
    __es_properties__ = {}


class Join(ESType):
    type_ = 'join'
    __es_properties__ = {}
