import datetime
import logging

import click
import pandas as pd
import tushare as ts

from tusharedb import config, db, util

api = ts.pro_api(token=config.TS_TOKEN)
logger = logging.getLogger(__name__)


def sync_daily():
    sate = db.StateDb()
    dates = [date for date in sate.list_daily()]
    apis = ['daily', 'adj_factor', 'daily_basic']
    # apis = ['daily_basic', ]
    # methods = {'daily': db.dbs[config.DT_DAILY_BFQ],
    #            'adj_factor': db.dbs[config.DT_DAILY_ADJFACTOR], 'daily_basic': db.dbs[config.DT_DAILY_BASIC]}
    with click.progressbar(pd.date_range(config.SYNC_START, datetime.datetime.now())) as bar:
        for date in bar:

            _date = date.strftime('%Y%m%d')
            # 重新收集近2天数据
            if _date in dates and date < (datetime.datetime.now() - datetime.timedelta(days=2)):
                continue
            for api_name in apis:
                logger.debug(
                    'fetch data from tushare, api_name: %s, trade_date: %s', api_name, _date)
                util.speed_it()
                df = api.query(api_name, trade_date=_date)
                if df.empty:
                    continue
                else:
                    api_type = config.API_TYPE_TRADE_DATE
                    dbcls_config = config.get_write_api_db(api_name, api_type)
                    dbcls = db.dbs[dbcls_config]
                    dbcls(_date).save(df)
            sate.append_daily(_date)


def _sync_code(start, end, apis, recent, ok_codes, callback, ):
    codes = api.stock_basic(list_status='L',
                            fields='ts_code')
    codes = codes.ts_code
    sate = db.StateDb()
    # codes = ['601600.SH', '601601.SH']
    with click.progressbar(codes) as bar:

        for code in bar:
            if code in ok_codes:
                logger.debug('pass %s', code)
                continue
            for api_name in apis:
                dfs = []
                start_date = start
                config_end_date = end
                for end_date in pd.date_range(start=start_date, end=config_end_date, freq='5Y'):
                    util.speed_it()
                    logger.debug('fetch data from tushare, api_name: %s, ts_code:%s, start_date: %s, end_date:%s',
                                 api_name, code, start_date.strftime('%Y%m%d'), end_date.strftime('%Y%m%d'))
                    dfs.append(api.query(api_name, ts_code=code,
                                         start_date=start_date.strftime('%Y%m%d'), end_date=end_date.strftime('%Y%m%d')))

                    start_date = end_date + datetime.timedelta(days=1)
                logger.debug('fetch data from tushare, api_name: %s, ts_code:%s, start_date: %s, end_date:%s',
                             api_name, code, start_date.strftime('%Y%m%d'), config_end_date.strftime('%Y%m%d'))
                util.speed_it()
                dfs.append(api.query(api_name, ts_code=code,
                                     start_date=start_date.strftime('%Y%m%d'), end_date=config_end_date.strftime('%Y%m%d')))

                df = pd.concat(dfs)
                if df.empty:
                    continue
                else:
                    db_config = config.get_write_api_db(
                        api_name, config.API_TYPE_TS_CODE, recent=recent)
                    dbcls = db.dbs[db_config]
                    dbcls(code).delete()
                    dbcls(code).save(df)

            callback(code)


def sync_code_history():
    sate = db.StateDb()

    start = datetime.datetime.strptime(
        config.SYNC_START, '%Y-%m-%d')
    end = datetime.datetime.strptime(
        sate.sync_code_history_end, '%Y-%m-%d')
    apis = ['daily', 'adj_factor', 'daily_basic']

    ok_codes = [date for date in sate.list_code()]
    _sync_code(start, end, apis, False, ok_codes, sate.append_code)


def sync_code_recent(nocache=True):

    sate = db.StateDb()
    start = datetime.datetime.strptime(
        sate.sync_code_history_end, '%Y-%m-%d') + datetime.timedelta(days=1)
    end = datetime.datetime.now()
    apis = ['daily', 'adj_factor', 'daily_basic']

    if nocache:
        logging.info('try to delete')
        sate.delete_recentcode()

    ok_codes = [date for date in sate.list_recentcode()]

    _sync_code(start, end, apis, True, ok_codes, sate.append_recentcode)

    # 正常结束，去掉cache记录
    sate.delete_recentcode()


def sync_index():
    api_names = ['index_daily', ]

    codes = ['399001.SZ', '399005.SZ', '399006.SZ', '000001.SH']
    # codes = ['601600.SH', '601601.SH']
    start = datetime.datetime.strptime(config.SYNC_START, '%Y-%m-%d')
    end = datetime.datetime.now()
    with click.progressbar(codes) as bar:
        for code in bar:
            for api_name in api_names:
                dfs = []
                start_date = start
                config_end_date = end
                for end_date in pd.date_range(start=start_date, end=config_end_date, freq='5Y'):
                    logger.debug('%s,%s,%s,%s', api_name,
                                 code, start_date, end_date)
                    util.speed_20_per_min()
                    dfs.append(api.query(api_name, ts_code=code,
                                         start_date=start_date.strftime('%Y%m%d'), end_date=end_date.strftime('%Y%m%d')))

                    start_date = end_date + datetime.timedelta(days=1)
                logger.debug('%s,%s,%s,%s', api_name,
                             code, start_date, config_end_date)
                util.speed_20_per_min()
                dfs.append(api.query(api_name, ts_code=code,
                                     start_date=start_date.strftime('%Y%m%d'), end_date=config_end_date.strftime('%Y%m%d')))

                df = pd.concat(dfs)
                df = df.sort_values('trade_date')
                df.index = pd.DatetimeIndex(df.trade_date)
                df = df.reindex(pd.date_range(
                    df.index[0], df.index[-1]), method='bfill')
                df = df.assign(trade_date=df.index.strftime('%Y%m%d'))
                if df.empty:
                    continue
                else:
                    dbcls_config = config.get_write_api_db(
                        api_name, config.API_TYPE_TS_CODE)
                    dbcls = db.dbs[dbcls_config]
                    dbcls(code).save(df)


def merge_code():
    pass


def sync_stock_basic():
    dbcls = db.dbs[config.get_write_api_db(
        'stock_basic', config.API_TYPE_NORMAL)]

    df = api.stock_basic(
        fields='ts_code,symbol,name,area,industry,fullname,enname,market,exchange,curr_type,list_status,list_date,delist_date,is_hs')
    dbcls().save(df)


if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.DEBUG)
    # sync_stock_basic()
    # sync_daily()
    # sync_code_history()
    # sync_code_recent()
    sync_index()
