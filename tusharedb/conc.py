import logging
from concurrent.futures import process, thread

import click
import pandas as pd

from tusharedb import api, config, db

process_pool = process.ProcessPoolExecutor(
    max_workers=config.PROCESS_POOL_SIZE)

logger = logging.getLogger(__name__)


class ConcurrentExecutor:
    '''
        考虑到leveldb只能单进程使用，而获取到数据 dataframe 后有大量的cpu计算，对df进行多进程处理
    '''

    def __init__(self, fetch, task, reduce):
        self.fetch = fetch
        self.task = task
        self.reduce = reduce

    def __call__(self, inkeys, **kwargs):

        with click.progressbar(inkeys, label='Fetch:') as bar:
            futures = []
            for key in bar:
                r = self.fetch(key, **kwargs)
                f = process_pool.submit(self.task, key, r, **kwargs)
                futures.append(f)
                # self.task(code, r)

        with click.progressbar(futures, label='Task :') as bar:
            results = []
            for f in bar:
                results.append(f.result())

        list_ = []
        for rec in results:
            if rec is not None:
                if isinstance(rec, (list, tuple)):
                    list_.extend(rec)
                else:
                    list_.append(rec)
        is_df = False
        for obj in list_:
            if isinstance(obj, pd.DataFrame):
                is_df = True
                break

        if is_df:
            df = pd.concat(list_)
        else:
            df = pd.DataFrame(data=list_)
        return self.reduce(df, **kwargs)


class JobResultDb(db.PrefixedDfDb):
    prefix = 'tsjob:'
    job_name = ''

    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        if not self.job_name:
            raise KeyError('job_name can not be None')

        self.db = self.db.prefixed_db(db.force_bytes(self.job_name + ':'))


class TsDbJob:
    job_name = ''

    def get_db_obj(self, db_key=None):
        cls_ = type('TsJob_'+self.job_name, (JobResultDb,),
                    {'job_name': self.job_name})
        obj = cls_()
        if db_key:
            obj.db.prefixed_db(db.force_bytes(db_key))
        return obj

    def to_db(self, df, db_key=None):

        job_db = self.get_db_obj(db_key)
        job_db.delete()
        job_db.save(df)

    def read_db(self, columns=None, db_key=None):
        job_db = self.get_db_obj(db_key)
        return job_db.read(columns)

    def fetch_bfq(self, ts_code, **kwargs):
        start_date = kwargs.get('start_date', None)
        end_date = kwargs.get('end_date', None)
        fields = kwargs.get('fields', None)
        df = api.bfq(ts_code=ts_code, start_date=start_date,
                     end_date=end_date, fields=fields)
        return df

    def fetch_bfq_by_trade_date(self, trade_date, **kwargs):
        fields = kwargs.get('fields', None)
        df = api.date_bfq(trade_date=trade_date, fields=fields)
        return df

    def fetch_daily_basic(self, ts_code, **kwargs):

        start_date = kwargs.get('start_date', None)
        end_date = kwargs.get('end_date', None)
        fields = kwargs.get('fields', None)
        df = api.api.daily_basic(ts_code=ts_code, start_date=start_date,
                                 end_date=end_date, fields=fields)
        return df

    def fetch_daily_basic_by_date(self, trade_date, **kwargs):

        start_date = kwargs.get('start_date', None)
        end_date = kwargs.get('end_date', None)
        fields = kwargs.get('fields', None)
        df = api.api.daily_basic(trade_date=trade_date,  fields=fields)
        return df

    def fetch(self, key, **kwargs):
        pass

    def handler(self, key, data, **kwargs):
        pass

    def reduce(self, results, **kwargs):
        pass

    def __call__(self, inkeys, **kwargs):
        executor = ConcurrentExecutor(self.fetch, self.handler, self.reduce)
        return executor(inkeys, **kwargs)


if __name__ == "__main__":

    def fetch(ts_code, start_date=None, end_date=None):

        df = api.pro_bar(
            ts_code=ts_code, start_date=start_date, end_date=end_date)

        return df

    def task(ts_code, df):
        import time
        time.sleep(0.1)
        return {'ts_code': ts_code, 'size': len(df)}

    def reduce(rsts):
        s = 0
        for r in rsts:
            s += r['size']
        return s

    executor = ConcurrentExecutor(fetch=fetch, task=task, reduce=reduce)
    codes = api.api.stock_basic(list_status='L',
                                fields='ts_code')
    codes = codes.ts_code
    import time

    s = time.time()

    executor(codes, )

    e = time.time()
