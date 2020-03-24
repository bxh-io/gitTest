import os
from functools import wraps

from sqlalchemy import create_engine
import sqlalchemy.event as sa_event

import json_util


def get_connection_string():
    if 'DB_HOSTNAME' in os.environ:
        if 'DB_PASSWORD' in os.environ:
            return 'postgresql://{}:{}@{}/{}'.format(
                os.environ.get('DB_USERNAME'),
                os.environ.get('DB_PASSWORD'),
                os.environ.get('DB_HOSTNAME'),
                os.environ.get('DB_DATABASE')
            )
        else:
            return 'postgresql://{}@{}/{}'.format(
                os.environ.get('DB_USERNAME'),
                os.environ.get('DB_HOSTNAME'),
                os.environ.get('DB_DATABASE')
            )
    else:
        return 'postgresql://ihop:ihop@db:6432/rxcheck'


def _set_connection_search_path(dbapi_connection, connection_record):
    """
    Respond to an engine's connection event.

    This is a shim so that we don't need to rely on the search path
    being correctly set on our database. We set the search path on a
    per-connection basis.
    """
    del connection_record
    dbapi_connection.cursor().execute('SET search_path TO "$user", public, rxcheck')


engine = create_engine(
    get_connection_string(), pool_recycle=120, json_serializer=json_util.to_json
)
sa_event.listen(engine, 'connect', _set_connection_search_path)


def transactional(f):
    @wraps(f)
    def wrapped(*args, **kwargs):
        if 'connection' in kwargs:
            # pass an existing transaction through to another method
            return f(kwargs.pop('connection'), *args, **kwargs)
        with engine.begin() as connection:
            return f(connection, *args, **kwargs)

    return wrapped
