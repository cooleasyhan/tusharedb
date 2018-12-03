# -*- coding: utf-8 -*-

"""Console script for tusharedb."""
import logging
import sys

import click

from tusharedb import sync as dbsync


@click.group()
@click.option('--debug/--no-debug', default=False)
def sync(debug):
    click.echo('Debug mode is %s' % ('on' if debug else 'off'))
    if debug:
        logging.basicConfig(level='DEBUG')
    else:
        logging.basicConfig(level='INFO')


@sync.command()
def sync_code_history(args=None):
    '''根据配置同步历史数据'''
    dbsync.sync_code_history()


@sync.command()
@click.option('--cache/--no-cache', default=True, help='记录进程，便于重试，正常结束，会清除cache记录')
def sync_daily(cache):
    '''增量同步数据'''

    click.echo('sync_daily')
    dbsync.sync_daily()
    click.echo('sync_stock_basic')
    dbsync.sync_stock_basic()

    dbsync.sync_code_history()

    if cache:
        click.echo('sync_code_recent with cache')
        dbsync.sync_code_recent(nocache=False)
    else:
        click.echo('sync_code_recent with cache')
        dbsync.sync_code_recent(nocache=True)


def main():
    sync()


if __name__ == "__main__":
    sys.exit(cli())  # pragma: no cover
