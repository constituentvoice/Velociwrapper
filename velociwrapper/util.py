import re
import elasticsearch
from .connection import VWConnection, VWConnectionError
from . import config

__all__ = ['all_subclasses', 'unset', 'VWUnset', 'VWDialect']


def all_subclasses(cls):
    """
    Generator that recursively yields all the subclasses of the passed in class
    Args:
        cls: class
    Returns: generator
    """
    for subclass in cls.__subclasses__():
        yield subclass
        for sub_subclass in all_subclasses(subclass):
            yield sub_subclass


class VWDialect(object):
    _dialect = None

    @classmethod
    def dialect(cls, force_check=False):
        if cls._dialect and not force_check:
            return cls._dialect

        try:
            cls._dialect = config.dialect
        except AttributeError:
            cls._dialect = None

        if not cls._dialect:
            # try via the connection
            try:
                connection = VWConnection.get_connection()
                info = connection.info()
                version = info.get('version').get('number')
                matches = re.match('^(\d+)\.', version)
                try:
                    cls._dialect = int(matches.group(1))
                except IndexError:
                    pass
            except VWConnectionError:
                cls._dialect = elasticsearch.__version__[0]

        return cls._dialect


class VWUnset(object):
    """
    An empty value.

    Used when None and False are valid choices to verify 
    the value is not set by the user

    Thanks to WTForms for this idea
    """

    def __repr__(self):
        return '<VWUnset>'

    def __bool__(self):
        return False

    def __nonzero__(self):
        return False


unset = VWUnset()
