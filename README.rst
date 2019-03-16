=========
Tusharedb
=========


.. image:: https://img.shields.io/pypi/v/tusharedb.svg
        :target: https://pypi.python.org/pypi/tusharedb

.. image:: https://img.shields.io/travis/cooleasyhan/tusharedb.svg
        :target: https://travis-ci.org/cooleasyhan/tusharedb

.. image:: https://readthedocs.org/projects/tusharedb/badge/?version=latest
        :target: https://tusharedb.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status




Cache Tushare Pro Api data to LevelDB


* Free software: MIT license
* Documentation: https://tusharedb.readthedocs.io.


Features
--------

* 一键同步历史
* 支持增量同步数据
* 支持命令行 tusharedb --help
* 支持批量处理数据

[由于tushare pro 增加了调用限制，1分钟只能发送150请求....]


* 提供Job封装
.. code-block:: python
    from tusharedb.api import api, pro_bar
    from tusharedb.conc import TsDbJob

    class TestJob(TsDbJob):

        def fetch(self, ts_code, start_date=None, end_date=None):
            df = pro_bar(ts_code=ts_code, start_date=start_date,
                        end_date=end_date)
            return df

        def handler(self, ts_code, df):
            import time

            if df is not None and not df.empty:
                return {'ts_code': ts_code, 'size': len(df)}

        def reduce(self, df):
            print('SUM:', sum(df['size']))
            return df


    def main():
        codes = api.stock_basic(list_status='L',
                                fields='ts_code')
        codes = codes.ts_code
        job = TestJob()
        df = job(codes[:100])


    if __name__ == "__main__":
        main()
        # 结果：
        # Fetch:  [####################################]  100%
        # SUM: 514767
        # use  2.0354549884796143


Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
