from __future__ import absolute_import, unicode_literals
from elasticsearch import Elasticsearch


class VWConnectionError(Exception):
    pass


class VWConnection(object):
    connections = {}
    default_connection = None

    @classmethod
    def connect(cls, dsn, set_default=False, **params):
        try:
            cls.connections[str(dsn)]
        except KeyError:
            cls.connections[str(dsn)] = Elasticsearch(dsn, **params)

        if set_default:
            cls.set_connection(cls.connections[str(dsn)])

        return cls.connections[str(dsn)]

    @classmethod
    def remove_connection(cls, dsn):
        try:
            del cls.connections[str(dsn)]
        except KeyError:
            pass

    @classmethod
    def get_connection(cls):
        if cls.default_connection:
            return cls.default_connection
        else:
            raise VWConnectionError('No default connection created. Call set connection first.')

    @classmethod
    def set_connection(cls, connection):
        cls.default_connection = connection
