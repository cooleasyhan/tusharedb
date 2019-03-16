import copy
import datetime
import logging
from functools import partial, wraps

import pandas as pd
import tushare as ts
from tushare.util.formula import MA

from tusharedb import config
from tusharedb.db import (PrefixedDb, PrefixedDfDb, dbs, force_bytes,
                          force_unicode)

PRICE_COLS = ['open', 'close', 'high', 'low']


def FORMAT(x): return float('%.2f' % x)


FREQS = {'D': '1DAY',
         'W': '1WEEK',
         'Y': '1YEAR',
         }


logger = logging.getLogger(__name__)


class DataApi:

    def __init__(self, ts_token=None):
        self.ts_token = ts_token if ts_token else config.TS_TOKEN
        self.pro = ts.pro_api(
            token=self.ts_token)
        self.dbcls = {}

    def delete(self, api_name):
        db = self.get_dbobj(api_name).delete()

    def get_dbobj(self, api_name, ts_code, trade_date):
        if ts_code:
            if api_name == 'daily':
                return dbs[config.DT_CODE_BFQ]

    def _query_db(self, api_name, **kwargs):
        ts_code = kwargs.get('ts_code', '')
        trade_date = kwargs.get('trade_date', '')
        fields = kwargs.get('fields', '')

        if ts_code:
            api_type = config.API_TYPE_TS_CODE
            key = ts_code
        elif trade_date:
            api_type = config.API_TYPE_TRADE_DATE
            key = trade_date
        else:
            api_type = config.API_TYPE_NORMAL
            key = None

        db_configs = config.get_api_db(api_name, api_type)
        dfs = []
        for db_config in db_configs:
            dbcls = dbs[db_config]
            if key:
                db = dbcls(key)
            else:
                db = dbcls()
            if fields:
                tmp1 = tmp2 = fields.split(',')

                extra_fields = config.get_api_extra_fields(
                    api_name, api_type, **kwargs)
                for f in extra_fields:
                    if f not in tmp2:
                        tmp1.append(f)

                df = db.read(tmp1)
            else:
                df = db.read()
            # print(df)
            dfs.append(df)

        if len(dfs) > 1:
            df = pd.concat(dfs, sort=False)
            df = config.filter_df(api_name, api_type, df, **kwargs)
            df = df.sort_values('trade_date', ascending=False)
            df.index = range(len(df))
        else:
            df = dfs[0]
            df = config.filter_df(api_name, api_type, df, **kwargs)

        if df is None:
            return None

        if fields:
            df = df[fields.split(',')]
        return df

        # if api_name in ('stock_basic',):
        #     if fields:
        #         df = db.read(fields.split(','))
        #     else:
        #         df = db.read()

        # if trade_date:
        #     if api_name == 'daily':
        #         dbcls = dbs[config.DT_DAILY_BFQ]
        #     elif api_name == 'adj_factor':
        #         dbcls = dbs[config.DT_DAILY_ADJFACTOR]
        #     elif api_name == 'daily_basic':
        #         dbcls = dbs[config.DT_DAILY_BASIC]
        #     elif api_name == 'index_daily':
        #         dbcls = dbs[config.DT_DAILY_INDEX]
        #     db = dbcls(trade_date)
        #     if fields:
        #         df = db.read(fields.split(','))
        #     else:
        #         df = db.read()

        #     if 'start_date' in kwargs and kwargs['start_date']:
        #         df = df[df['trade_date'] >= kwargs['start_date']]
        #     if 'end_date' in kwargs and kwargs['end_date']:
        #         df = df[df['trade_date'] <= kwargs['end_date']]
        #     if ts_code:
        #         df = df[df['ts_code'] == ts_code]

        #     return df

        # if ts_code:
        #     if api_name == 'daily':
        #         dbcls = (dbs[config.DT_CODE_RECENTBFQ], dbs[config.DT_CODE_BFQ],
        #                  )
        #     elif api_name == 'adj_factor':
        #         dbcls = (dbs[config.DT_CODE_RECENTADJFACTOR], dbs[config.DT_CODE_ADJFACTOR],
        #                  )
        #     elif api_name == 'daily_basic':
        #         dbcls = (dbs[config.DT_CODE_RECENTBASIC], dbs[config.DT_CODE_BASIC],
        #                  )
        #     elif api_name == 'index_daily':
        #         dbcls = (dbs[config.DT_CODE_INDEX],)
        #     dfs = []
        #     for tmp in dbcls:
        #         db = tmp(ts_code)
        #         if fields:
        #             df = db.read(fields.split(','))
        #         else:
        #             df = db.read()

        #         dfs.append(df)

        #     df = pd.concat(dfs)

        #     if 'start_date' in kwargs and kwargs['start_date']:
        #         df = df[df['trade_date'] >= kwargs['start_date']]
        #     if 'end_date' in kwargs and kwargs['end_date']:
        #         df = df[df['trade_date'] <= kwargs['end_date']]
        #     df = df.sort_values('trade_date', ascending=False)
        #     df.index = range(len(df))
        #     return df

    def query(self, api_name, **kwargs):
        if api_name not in config.APIS:
            logger.debug('%s, fetch data from tushare api', api_name)
            return self.pro.query(api_name, **kwargs)

        logger.debug('%s, fetch data from tushare db', api_name)

        try:
            df = self._query_db(api_name, **kwargs)
            return df

        except Exception as e:
            logger.exception(e)
            df = None
            logger.warning(
                '%s(%s) fetch data from tushare db error, try to fetch data from tushare api', api_name, kwargs)

            return self.pro.query(api_name, **kwargs)

    def __getattr__(self, name):
        return partial(self.query, name)


api = DataApi(ts_token=config.TS_TOKEN)


def bfq(ts_code='', start_date=None, end_date=None, freq='D', include_factor=True, fields=None):
    '''
    按ts code获取bfq数据
    '''
    df = api.daily(ts_code=ts_code,
                   start_date=start_date, end_date=end_date, fields=fields)
    if df.empty:
        return df
    if include_factor:
        fcts = api.adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)[
            ['trade_date', 'adj_factor']]
        data = df.set_index('trade_date', drop=False).merge(fcts.set_index(
            'trade_date'), left_index=True, right_index=True, how='left')
        data['adj_factor'] = data['adj_factor'].fillna(
            method='bfill')
        return data


def date_bfq(trade_date,  freq='D', include_factor=True, fields=None):
    '''
    按天获取所有bfq数据
    '''
    df = api.daily(trade_date=trade_date,
                   fields=fields)
    if df is None:
        return None
    if df.empty:
        return df
    if include_factor:
        fcts = api.adj_factor(trade_date=trade_date)[
            ['ts_code', 'adj_factor']]
        data = df.set_index('ts_code', drop=False).merge(fcts.set_index(
            'ts_code'), left_index=True, right_index=True, how='left')
        data['adj_factor'] = data['adj_factor'].fillna(
            method='bfill')
        return data


def adj(bfq, start_date=None, end_date=None, adj=None):

    if start_date:
        data = bfq[bfq['trade_date'] >= start_date]
    else:
        data = copy.copy(bfq)

    if end_date:
        data = data[data['trade_date'] <= end_date]

    if not data.empty:
        for col in PRICE_COLS:
            if col not in data:
                continue
            if adj == 'hfq':
                data[col] = data[col] * data['adj_factor']
            else:
                data[col] = data[col] * data['adj_factor'] / \
                    float(data['adj_factor'][0])
            data[col] = data[col].map(FORMAT)
    return data


def pro_bar(ts_code='', start_date=None, end_date=None, freq='D', asset='E',
            market='',
            adj=None,
            ma=[],
            retry_count=3):
    """
    BAR数据
    Parameters:
    ------------
    ts_code:证券代码，支持股票,ETF/LOF,期货/期权,港股,数字货币
    start_date:开始日期  YYYYMMDD
    end_date:结束日期 YYYYMMDD
    freq:支持1/5/15/30/60分钟,周/月/季/年
    asset:证券类型 E:股票和交易所基金，I:沪深指数,C:数字货币,F:期货/期权/港股/中概美国/中证指数/国际指数
    market:市场代码，通过ts.get_markets()获取
    adj:复权类型,None不复权,qfq:前复权,hfq:后复权
    ma:均线,支持自定义均线频度，如：ma5/ma10/ma20/ma60/maN
    factors因子数据，目前支持以下两种：
        vr:量比,默认不返回，返回需指定：factor=['vr']
        tor:换手率，默认不返回，返回需指定：factor=['tor']
                    以上两种都需要：factor=['vr', 'tor']
    retry_count:网络重试次数

    Return
    ----------
    DataFrame
    code:代码
    open：开盘close/high/low/vol成交量/amount成交额/maN均价/vr量比/tor换手率

         期货(asset='X')
    code/open/close/high/low/avg_price：均价  position：持仓量  vol：成交总量
    """
    ts_code = ts_code.strip().upper()

    for _ in range(retry_count):
        try:
            freq = freq.strip().upper()
            asset = asset.strip().upper()
            if asset == 'E':
                if freq == 'D':
                    df = api.daily(ts_code=ts_code,
                                   start_date=start_date, end_date=end_date)
                    if df.empty:
                        return df
                    if adj is not None:
                        fcts = api.adj_factor(ts_code=ts_code, start_date=start_date, end_date=end_date)[
                            ['trade_date', 'adj_factor']]
                        data = df.set_index('trade_date', drop=False).merge(fcts.set_index(
                            'trade_date'), left_index=True, right_index=True, how='left')
                        data['adj_factor'] = data['adj_factor'].fillna(
                            method='bfill')
                        for col in PRICE_COLS:
                            if col in data:
                                if adj == 'hfq':
                                    data[col] = data[col] * data['adj_factor']
                                else:
                                    data[col] = data[col] * data['adj_factor'] / \
                                        float(fcts['adj_factor'][0])
                                data[col] = data[col].map(FORMAT)
                        data = data.drop('adj_factor', axis=1)
                    else:
                        data = df
                    if ma is not None and len(ma) > 0:
                        for a in ma:
                            if isinstance(a, int):
                                data['ma%s' % a] = MA(data['close'], a).map(
                                    FORMAT).shift(-(a-1))
                                data['ma%s' % a] = data['ma%s' %
                                                        a].astype(float)
                    for col in PRICE_COLS:
                        data[col] = data[col].astype(float)
            if asset == 'I':
                if freq == 'D':
                    data = api.index_daily(
                        ts_code=ts_code, start_date=start_date, end_date=end_date)
            if asset == 'C':
                # //////////////////////// well soon
                pass
            return data
        except Exception as e:
            logger.exception(e)
            return None
        else:
            return
    raise IOError('ERROR.')


def index_daily(ts_code=None, trade_date=None, start_date=None, end_date=None, online=True):
    api_name = 'index_daily'
    if not start_date:
        start_date = datetime.datetime.strptime(config.SYNC_START, '%Y-%m-%d')
    if not end_date:
        end_date = datetime.datetime.now()

    if online:
        _api = api.pro
    else:
        _api = api

    dfs = []
    for _end_date in pd.date_range(start=start_date, end=end_date, freq='5Y'):
        logger.debug('%s,%s,%s,%s', api_name,
                     ts_code, start_date, end_date)
        dfs.append(_api.query(api_name, ts_code=ts_code, trade_date=trade_date,
                              start_date=start_date.strftime('%Y%m%d'), end_date=_end_date.strftime('%Y%m%d')))

        start_date = _end_date + datetime.timedelta(days=1)
    logger.debug('%s,%s,%s,%s', api_name,
                 ts_code, start_date, end_date)
    dfs.append(_api.query(api_name, ts_code=ts_code, trade_date=trade_date,
                          start_date=start_date.strftime('%Y%m%d'), end_date=end_date.strftime('%Y%m%d')))

    df = pd.concat(dfs)
    df = df.sort_values('trade_date')
    return df


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    # df = api.daily(ts_code='601600.SH',
    #                start_date='20160101', end_date='20170301')
    # # print(df)

    # df = api.daily(trade_date='20181114')
    # # print(df)

    # df = api.stock_basic(list_status='L', is_hs='N',
    #                      fields='is_hs')
    # print(df)
    df = api.daily_basic(trade_date='20181116')
    print(df.pe)
