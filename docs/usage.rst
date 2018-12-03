=====
Usage
=====

To use Tusharedb in a project::


    from tusharedb.api import api, pro_bar
    from tusharedb.conc import TsDbJob


    class TestJob(TsDbJob):

        def fetch(self, ts_code, start_date=None, end_date=None):
            df = pro_bar(ts_code=ts_code, start_date=start_date, end_date=end_date)
            return df

        def handler(self, ts_code, df):
            import time

            if df is not None and not df.empty:
                return {'ts_code': ts_code, 'size': len(df)}

        def reduce(self, df):
            print(sum(df['size']))

    def main():
        codes = api.stock_basic(list_status='L',
                                fields='ts_code')
        codes = codes.ts_code
        task = TestJob()

        task(codes[:10])

    if __name__ == "__main__":
        main()
