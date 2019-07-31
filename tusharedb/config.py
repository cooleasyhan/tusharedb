import collections
import os

TS_TOKEN = os.environ.get('TS_TOKEN')
LEVEL_DB_NAME = os.environ.get('LEVEL_DB_NAME')

SYNC_START = '1990-12-19'
SYNC_CODE_HISTORY_END = '2019-01-01'

PROCESS_POOL_SIZE = int(os.environ.get(
    'TS_PROCESS_POOL_SIZE', os.cpu_count())) or 1
THREAD_POOL_SIZE = 5


LIST_STATUS = ('L', 'D', 'P')  # 上市状态： L上市 D退市 P暂停上市
EXCHANGE = ('SSE', 'SZSE', 'HKEX')  # 交易所 SSE上交所 SZSE深交所 HKEX港交所
IS_HS = ('N', 'H', 'S')  # 是否沪深港通标的，N否 H沪股通 S深股通

DT_DAILY_BFQ = 'daily:bfq'
DT_DAILY_ADJFACTOR = 'daily:adjfactor'
DT_DAILY_BASIC = 'daily:basic'

DT_CODE_BFQ = 'code:bfq'
DT_CODE_BASIC = 'code:base'
DT_CODE_ADJFACTOR = 'code:adjfactor'

DT_CODE_RECENTBFQ = 'code:recentbfq'
DT_CODE_RECENTBASIC = 'code:recentbase'
DT_CODE_RECENTADJFACTOR = 'code:recentadjfactor'

DT_CODE_INDEX = 'code:index'
DT_DAILY_INDEX = 'daily:index'

DT_NORMAL_STOCK_BASIC = 'normal:stockbasic'

DT_NEWS = 'daily:news'


APISETUPS = collections.defaultdict(dict)

API_TYPE_NORMAL = 'normal'
API_TYPE_TS_CODE = 'ts_code'
API_TYPE_TRADE_DATE = 'ts_date'


def setup_api(api_name,  db=None, ts_code_db=None, trade_date_db=None, filters=None):
    '''
    @api_type: normal, ts_code, trade_date
    '''
    _filters = filters if filters else []
    append_cols = [f for f in _filters if f not in ('start_date', 'end_date')]

    if not db and not ts_code_db and not trade_date_db:
        raise ValueError('db ts_code_db trade_date_db 必选一个')

    if db:
        key = '%s:%s' % (api_name, API_TYPE_NORMAL)
        if isinstance(db, (tuple, list)):
            APISETUPS[key]['db'] = db
        else:
            APISETUPS[key]['db'] = (db,)

        APISETUPS[key]['filters'] = _filters
        APISETUPS[key]['append_cols'] = append_cols

    if ts_code_db:
        key = '%s:%s' % (api_name, API_TYPE_TS_CODE)
        if isinstance(ts_code_db, (tuple, list)):
            APISETUPS[key]['ts_code_db'] = ts_code_db
        else:
            APISETUPS[key]['ts_code_db'] = (ts_code_db,)
        APISETUPS[key]['filters'] = _filters
        APISETUPS[key]['append_cols'] = append_cols

    if trade_date_db:
        key = '%s:%s' % (api_name, API_TYPE_TRADE_DATE)
        if isinstance(trade_date_db, (tuple, list)):
            APISETUPS[key]['trade_date_db'] = trade_date_db
        else:
            APISETUPS[key]['trade_date_db'] = (trade_date_db,)
        APISETUPS[key]['filters'] = _filters
        APISETUPS[key]['append_cols'] = append_cols


setup_api('stock_basic', db=DT_NORMAL_STOCK_BASIC,
          filters=['is_hs', 'list_status', 'exchange'])

setup_api('daily', ts_code_db=(DT_CODE_BFQ, DT_CODE_RECENTBFQ),
          trade_date_db=DT_DAILY_BFQ, filters=['start_date', 'end_date'])

setup_api('adj_factor', ts_code_db=(DT_CODE_ADJFACTOR, DT_CODE_RECENTADJFACTOR),
          trade_date_db=DT_DAILY_ADJFACTOR, filters=['start_date', 'end_date'])

setup_api('daily_basic', ts_code_db=(DT_CODE_BASIC, DT_CODE_RECENTBASIC),
          trade_date_db=DT_DAILY_BASIC, filters=['start_date', 'end_date'])

setup_api('index_daily', ts_code_db=(DT_CODE_INDEX,),
          trade_date_db=DT_DAILY_INDEX, filters=['start_date', 'end_date'])

setup_api('news', ts_code_db=(), trade_date_db=DT_NEWS,
          filters=['start_date', 'end_date'])


def get_api_setup(api_name, api_type):
    key = '%s:%s' % (api_name, api_type)
    return APISETUPS[key]


def get_api_extra_fields(api_name, api_type, **kwargs):
    key = '%s:%s' % (api_name, api_type)
    appends_fields = APISETUPS[key]['append_cols']
    return [f for f in appends_fields if f in kwargs]


def get_api_db(api_name, api_type):
    s = get_api_setup(api_name, api_type)
    if api_type == API_TYPE_NORMAL:
        return s['db']
    elif api_type == API_TYPE_TRADE_DATE:
        return s['trade_date_db']
    elif api_type == API_TYPE_TS_CODE:
        return s['ts_code_db']


def get_read_api_db(api_name, api_type):
    return get_api_db(api_name, api_type)


def get_write_api_db(api_name, api_type, recent=True):
    dbs = get_api_db(api_name, api_type)
    if recent:
        return dbs[-1]
    else:
        return dbs[0]


def filter_df(api_name, api_type, df, **kwargs):
    filters = get_api_setup(api_name, api_type)['filters']
    for f in filters:
        if f == 'start_date' and 'start_date' in kwargs and kwargs['start_date']:
            df = df[df['trade_date'] >= kwargs['start_date']]
        elif f == 'end_date' and 'end_date' in kwargs and kwargs['end_date']:
            df = df[df['trade_date'] <= kwargs['end_date']]
        else:
            if f in kwargs and kwargs[f]:
                df = df[df[f] == kwargs[f]]
    return df


APIS = [k.split(':')[0] for k in APISETUPS.keys()]
