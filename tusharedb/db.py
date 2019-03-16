import copy
import logging
import pickle
from functools import partial
from threading import Lock

import pandas as pd
import plyvel

from tusharedb import config

LEVEL_DB_NAME = config.LEVEL_DB_NAME
LEVEL_DBS = {}

lock = Lock()


def force_bytes(s):
    try:
        return s.encode()
    except:
        return s


def force_unicode(s):
    try:
        return s.decode()
    except:
        return s


class Db:
    name = LEVEL_DB_NAME

    def __init__(self,  **kwargs):
        global LEVEL_DB
        with lock:
            if not self.name in LEVEL_DBS:
                LEVEL_DBS[self.name] = plyvel.DB(
                    self.name, create_if_missing=True, ** kwargs)
            self.db = LEVEL_DBS[self.name]

    def close(self):
        del LEVEL_DBS[self.name]
        if isinstance(self.db, plyvel.DB):
            self.db.close()
        else:
            self.db.db.close()

    def keys(self):
        return self.db.iterator(include_value=False)

    def values(self):
        return self.db.iterator(include_key=False)

    def empty(self):
        for key in self.db.iterator(include_value=False):
            return False

        return True

    def items(self):
        return self.db.iterator()


class PrefixedDb(Db):
    prefix = ''

    def __init__(self,  **kwargs):
        super().__init__(**kwargs)
        if self.prefix:
            self.db = self.db.prefixed_db(force_bytes(self.prefix))
        else:
            raise ValueError('prefix can not be none')


class DfDb(Db):
    index_col = 'index'

    def pre_save(self, df):
        return df

    def saved_index(self):
        v = self.db.get(force_bytes(self.index_col))
        if v:
            return pickle.loads(v)
        else:
            return None

    def save(self, df):
        df = self.pre_save(df)
        with self.db.write_batch() as wb:
            for column in df.columns:
                v = df[column]
                self.db.put(column.encode(),
                            pickle.dumps(v.values))
            self.db.put(force_bytes(self.index_col), pickle.dumps(df.index))

    def delete(self, columns=None):
        with self.db.write_batch() as wb:
            for column in columns if columns else self.keys():
                self.db.delete(force_bytes(column))
            if columns is None:
                self.db.delete(force_bytes(self.index_col))

    def read(self, columns=None):
        _columns = columns if columns else self.keys()
        serieses = list()
        tmp = dict()
        if self.empty():
            return
        for c in _columns:
            if c == force_bytes(self.index_col):
                continue
            try:

                v = pickle.loads(
                    self.db.get(force_bytes(c)))
            except TypeError:
                logging.error('Error: get %s from db %s' % (c, self.db))
                raise

            tmp[force_unicode(c)] = v
            index = pickle.loads(self.db.get(force_bytes(self.index_col)))
        if tmp:
            # print(tmp, index)
            df = pd.DataFrame(data=tmp, index=index)
            df = self.handler_result(df)
            return df
        else:
            return None

    def handler_result(self, df):
        return df


class PrefixedDfDb(DfDb):
    prefix = ''

    def __init__(self,  **kwargs):
        super().__init__(**kwargs)
        if self.prefix:
            self.db = self.db.prefixed_db(force_bytes(self.prefix))
        else:
            raise ValueError('prefix can not be none')


class StockBasicDb(PrefixedDfDb):
    prefix = 'ts:stockbasic'


# class DailyBaseDb(PrefixedDfDb):
#     prefix = 'ts:daily:'

#     def __init__(self, day=None, **kwargs):
#         super().__init__(**kwargs)
#         if day:
#             self.db = self.db.prefixed_db(force_bytes(day+':'))
#         self.day = day


# class _CodeBaseDb(PrefixedDfDb):
#     prefix = 'ts:code:'

#     def __init__(self, code=None, **kwargs):
#         super().__init__(**kwargs)
#         if code:
#             self.db = self.db.prefixed_db(force_bytes(code+':'))
#         self.code = code


class StateDb(PrefixedDb):
    prefix = 'ts:state:'
    logger = logging.getLogger('StateDb')

    def _db(self, name):
        return self.db.prefixed_db(force_bytes(name + ':'))

    def _keys(self, name):
        for key in self._db(name).iterator(include_value=False):
            yield force_unicode(key)

    def _append(self, name, value):
        self.logger.debug('Add %s to db %s', value, name)
        self._db(name).put(force_bytes(value), force_bytes('1'))

    def _delete(self, name):
        self.logger.debug('Delete all keys in db %s', name)
        with self._db(name).write_batch() as b:
            for key in self._keys(name):
                b.delete(force_bytes(key))

    def __getattr__(self, name):
        if name.startswith('append_'):
            return partial(self._append, name[7:])
        elif name.startswith('list_'):
            return partial(self._keys, name[5:])
        elif name.startswith('delete_'):
            return partial(self._delete, name[7:])

    @property
    def sync_code_history_end(self):
        tmp = self.db.get(force_bytes('sync_code_history_end'))
        if tmp:
            return force_unicode(tmp)
        else:
            return config.SYNC_CODE_HISTORY_END

    @sync_code_history_end.setter
    def set_sync_code_history_end(self, value):
        self.logger.info('set sync_code_history_end to %s', value)
        self.db.put(force_bytes('sync_code_history_end'), force_bytes(value))


# class DailyCodeDb(DailyBaseDb):
#     prefix = 'ts:daily:bfq:'


# class DailyAdjFactor(DailyBaseDb):
#     prefix = 'ts:daily:adjfactor:'


# class DailyBasicDb(DailyBaseDb):
#     prefix = 'ts:daily:basic:'


# class CodeBfqDb(_CodeBaseDb):
#     prefix = 'ts:code:bfq:'


# class CodeAdjFactorDb(_CodeBaseDb):
#     prefix = 'ts:code:adjfactor:'


# class CodeBaseDb(_CodeBaseDb):
#     prefix = 'ts:code:base:'


# class CodeBfqRecentDb(_CodeBaseDb):
#     prefix = 'ts:code:recentbfq:'


# class CodeAdjFactorRecentDb(_CodeBaseDb):
#     prefix = 'ts:code:recentadjfactor:'


# class CodeBaseRecentDb(_CodeBaseDb):
#     prefix = 'ts:code:recentbase:'


class _BaseDb(PrefixedDfDb):
    prefix = ''

    def __init__(self, key=None, **kwargs):
        super().__init__(**kwargs)
        if key:
            self.db = self.db.prefixed_db(force_bytes(key+':'))
        self.key = key


class DbHandler:
    def __init__(self):
        self._dbs = {}
        # self.valid_prefix = ('ts:daily:bfq:', 'ts:daily:adjfactor:', 'ts:daily:basic:',
        #                      'ts:code:bfq:', 'ts:code:base:',  'ts:code:adjfactor:',
        #                      'ts:code:recentbfq:', 'ts:code:recentadjfactor:', 'ts:code:recentbase:', 'ts:code:index:')

    def _get_dbcls(self, db_type, data_type):
        prefix = 'ts:%s:%s:' % (db_type, data_type)
        # if prefix not in self.valid_prefix:
        #     raise ValueError('db cls config %s not valid' % prefix)
        _cls = type('TsDb_'+prefix, (_BaseDb,), {'prefix': prefix})

        return _cls

    def __getitem__(self, alias):

        if alias not in self._dbs:
            self._dbs[alias] = self._get_dbcls(
                alias.split(':')[0], alias.split(':')[1])
        # for key, db in self._dbs.items():
        #     print(key, db, id(db), db.prefix)
        return self._dbs[alias]


dbs = DbHandler()


if __name__ == "__main__":
    # db = StateDb()
    # db.append_x('x')
    # db.append_x('y')
    # db.append_x('y')
    # print([k for k in db.list_x()])
    # print(db.sync_code_history_end)
    # # print([k for k in db.list_code()])
    # db = dbs['daily:bfq']()

    # print(db.read())
    db = dbs[config.DT_CODE_RECENTBFQ]
    # print(db.prefix)
    for k in db().keys():

        print(k)
    # df = db().read()

    # print(df)
