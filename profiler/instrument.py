import inspect
from datetime import datetime

from django.conf import settings
from django.db.models.sql.compiler import SQLCompiler
from django.db.models.sql.datastructures import EmptyResultSet
from django.db.models.sql.constants import MULTI

from aggregate.client import get_client

from profiler import _get_current_view


def format_path(string):
    return string.replace(settings.APP_ROOT, '').replace('/../eventsquare/', '')[0:-3].replace('/', '.')


def execute_sql(self, *args, **kwargs):
    client = get_client()
    if client is None:
        return self.__execute_sql(*args, **kwargs)
    try:
        q, params = self.as_sql()
        if not q:
            raise EmptyResultSet
    except EmptyResultSet:
        if kwargs.get('result_type', MULTI) == MULTI:
            return iter([])
        else:
            return
    start = datetime.now()
    try:
        return self.__execute_sql(*args, **kwargs)
    finally:
        d = (datetime.now() - start)
        # TODO: make this more generalized. good enough for what we need now.
        # tries to find where the sql call was made from
        our_stack = [
            '{module}.{func} #{num}'.format(module=format_path(f[1]), num=f[2], func=f[3])
            for f in inspect.stack() if 'eventsquare' in f[1]
        ]

        if our_stack:
            q = ' -> '.join(our_stack) + ' | ' + q

        client.insert({'query': q, 'view': _get_current_view(), 'type': 'sql'},
                      {'time': 0.0 + d.seconds * 1000 + d.microseconds/1000, 'count': 1})

INSTRUMENTED = False


if not INSTRUMENTED:
    SQLCompiler.__execute_sql = SQLCompiler.execute_sql
    SQLCompiler.execute_sql = execute_sql
    INSTRUMENTED = True
