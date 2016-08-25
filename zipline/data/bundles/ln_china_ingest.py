# coding=utf-8

import os

import numpy as np
import pandas as pd
from pandas_datareader.data import DataReader
import requests
import datetime
import MySQLdb
import MySQLdb.cursors
import numpy
import pandas

from zipline.utils.cli import maybe_show_progress


def __read_custom_datasource(symbol, cursor_gangao):
    """
    从前复权数据库中读取行情，填充到pandas
    :param symbol:
    :return:
    """
    #conn_gangao = MySQLdb.connect(host='yf-cbg-fb-gushitong14.yf01.baidu.com',
    #                              user='ln',
    #                                   port=8081,
    #                                   passwd='lnpw', db='stock_ana',
    #                                   charset='utf8')
    if symbol.startswith('6') or symbol.startswith('zs.sh'):
        tabname = 'sh_kline_day_complexbeforeright'
    else:
        tabname = 'sz_kline_day_complexbeforeright'
    # 取交易代码
    symbol = symbol.split('.')[-1]
    query_sql = '''
        select taba.* from
        (select * from jingong_work.{0} where symbol='{1}' and date>=20050101) taba
        join
        (select distinct date_format(TradingDay,'%Y%m%d') TradingDay from gangao.PUB_TRADINGDAY where exchangecode=101) tabb
        on taba.date=tabb.TradingDay
    '''.format(tabname, symbol)
    cursor_gangao.execute(query_sql)
    index = list()
    org_data = list()
    for row in cursor_gangao.fetchall():
        index.append(
            datetime.datetime.strptime(str(row['date']), '%Y%m%d')
        )
        org_data.append([
            numpy.float64(row['open']   ) ,
            numpy.float64(row['high']   ) ,
            numpy.float64(row['low']    ) ,
            numpy.float64(row['close']  ) ,
            numpy.float64(row['volume'] ) ,
            numpy.float64(row['close']  ) ])
    # conn_gangao.close()
    # cursor_gangao.close()
    if len(org_data) > 0:
        data = numpy.array(org_data)
    else:
        data = None
    df_true = pandas.DataFrame(index=index, columns=['Open', 'High', 'Low', 'Close', 'Volume', 'Adj Close'], data=data)
    df_true.index.name = 'Date'
    return df_true


def _cachpath(symbol, type_):
    return '-'.join((symbol.replace(os.path.sep, '_'), type_))


def china_equities(symbols, start=None, end=None):
    """Create a data bundle ingest function from a set of symbols loaded from
    yahoo.

    Parameters
    ----------
    symbols : iterable[str]
        The ticker symbols to load data for.
    start : datetime, optional
        The start date to query for. By default this pulls the full history
        for the calendar.
    end : datetime, optional
        The end date to query for. By default this pulls the full history
        for the calendar.

    Returns
    -------
    ingest : callable
        The bundle ingest function for the given set of symbols.

    Examples
    --------
    This code should be added to ~/.zipline/extension.py

    .. code-block:: python

       from zipline.data.bundles import yahoo_equities, register

       symbols = (
           'AAPL',
           'IBM',
           'MSFT',
       )
       register('my_bundle', yahoo_equities(symbols))

    Notes
    -----
    The sids for each symbol will be the index into the symbols sequence.
    """
    # strict this in memory so that we can reiterate over it
    symbols = tuple(symbols)

    def ingest(environ,
               asset_db_writer,
               minute_bar_writer,  # unused
               daily_bar_writer,
               adjustment_writer,
               calendar,
               cache,
               show_progress,
               output_dir,
               # pass these as defaults to make them 'nonlocal' in py2
               start=start,
               end=end):
        if start is None:
            start = calendar[0]
        if end is None:
            end = None

        metadata = pd.DataFrame(np.empty(len(symbols), dtype=[
            ('start_date', 'datetime64[ns]'),
            ('end_date', 'datetime64[ns]'),
            ('auto_close_date', 'datetime64[ns]'),
            ('symbol', 'object'),
        ]))

        def _pricing_iter():
            sid = 0
            with maybe_show_progress(
                    symbols,
                    show_progress,
                    label='Downloading Yahoo pricing data: ') as it, \
                    requests.Session() as session, \
                    MySQLdb.connect(host='yf-cbg-fb-gushitong14.yf01.baidu.com',
                        user='ln',
                        port=8081,
                        passwd='lnpw', db='stock_ana',
                        charset='utf8', cursorclass=MySQLdb.cursors.DictCursor) as cursor_gangao:
                for symbol in it:
                    print symbol
                    path = _cachpath(symbol, 'ohlcv')
                    try:
                        df = cache[path]
                    except KeyError:
                        # lnmark, load data.
                        read_data = __read_custom_datasource(symbol, cursor_gangao).sort_index()
                        if len(read_data) == 0:
                            continue
                        else:
                            df = cache[path] = read_data

                    # the start date is the date of the first trade and
                    # the end date is the date of the last trade
                    start_date = df.index[0]
                    end_date = df.index[-1]
                    # The auto_close date is the day after the last trade.
                    ac_date = end_date + pd.Timedelta(days=1)
                    metadata.iloc[sid] = start_date, end_date, ac_date, symbol

                    df.rename(
                        columns={
                            'Open': 'open',
                            'High': 'high',
                            'Low': 'low',
                            'Close': 'close',
                            'Volume': 'volume',
                        },
                        inplace=True,
                    )
                    yield sid, df
                    sid += 1

        daily_bar_writer.write(_pricing_iter(), show_progress=True)

        symbol_map = pd.Series(metadata.symbol.index, metadata.symbol)
        asset_db_writer.write(equities=metadata)

    #    adjustments = []
    #    with maybe_show_progress(
    #            symbols,
    #            show_progress,
    #            label='Downloading Yahoo adjustment data: ') as it, \
    #            requests.Session() as session:
    #        for symbol in it:
    #            path = _cachpath(symbol, 'adjustment')
    #            try:
    #                df = cache[path]
    #            except KeyError:
    #                # lnmark, load adjustment price
    #                df = cache[path] = DataReader(
    #                    symbol,
    #                    'yahoo-actions',
    #                    start,
    #                    end,
    #                    session=session,
    #                ).sort_index()
#
    #            df['sid'] = symbol_map[symbol]
    #            adjustments.append(df)
#
    #    adj_df = pd.concat(adjustments)
    #    adj_df.index.name = 'date'
    #    adj_df.reset_index(inplace=True)
#
    #    splits = adj_df[adj_df.action == 'SPLIT']
    #    splits = splits.rename(
    #        columns={'value': 'ratio', 'date': 'effective_date'},
    #    )
    #    splits.drop('action', axis=1, inplace=True)
#
    #    dividends = adj_df[adj_df.action == 'DIVIDEND']
    #    dividends = dividends.rename(
    #        columns={'value': 'amount', 'date': 'ex_date'},
    #    )
    #    dividends.drop('action', axis=1, inplace=True)
    #    # we do not have this data in the yahoo dataset
    #    dividends['record_date'] = pd.NaT
    #    dividends['declared_date'] = pd.NaT
    #    dividends['pay_date'] = pd.NaT
#
        adjustment_writer.write(splits=None, dividends=None)
#
    return ingest
