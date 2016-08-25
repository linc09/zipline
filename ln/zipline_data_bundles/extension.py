from zipline.data.bundles import yahoo_equities, register, unregister
from zipline.data.bundles.ln_china_ingest import china_equities

import MySQLdb
import pandas
import pytz

symbols = (
   'AAPL',
   'IBM',
   'MSFT',
)
register('my_bundle', yahoo_equities(symbols))

# register China stocks
conn_gangao = MySQLdb.connect(host='yf-cbg-fb-gushitong14.yf01.baidu.com',
                              user='ln',
                              port=8081,
                              passwd='lnpw', db='gangao',
                              charset='utf8')
cursor_gangao = conn_gangao.cursor(cursorclass=MySQLdb.cursors.DictCursor)

# get stock list
#query_sql = '''
#    select tradingcode from STK_BASICINFO taba join
#    (select SecuCode from STK_LISTINGSTATUS where NewStatusCode=1
#    ) tabb
#    on taba.SecuCode=tabb.SecuCode
#    where taba.exchangecode in(101,105) and taba.tradingcode regexp '^[0-9]+$'
#    ;
#'''
query_sql = '''
    select tradingcode from STK_BASICINFO taba
    where taba.exchangecode in(101,105) and taba.tradingcode regexp '^[0-9]+$'
    ;
'''
symbols_china = list()
cursor_gangao.execute(query_sql)
for row in cursor_gangao.fetchall():
    tc = row['tradingcode']
    if tc is not None:
        if tc.startswith('6') or tc.startswith('3') or tc.startswith('0'):
            symbols_china.append(row['tradingcode'])

# 增加需要使用的指数的数据
# 上证指数
symbols_china.append('zs.sh.000001')
# 沪深300
symbols_china.append('zs.sz.399300')
# 上证50
symbols_china.append('zs.sh.000016')
# 深证成指
symbols_china.append('zs.sz.399001')
# 创业板指
symbols_china.append('zs.sz.399006')
# 中证500
symbols_china.append('zs.sz.399905')


query_sql = '''
    select distinct TradingDay from PUB_TRADINGDAY where exchangecode in(101,105) order by TradingDay;
'''
cursor_gangao.execute(query_sql)
traday_list = list()
for row in cursor_gangao.fetchall():
    traday_list.append(row['TradingDay'])

cursor_gangao.close()
conn_gangao.close()

calendar = pandas.DatetimeIndex(data=traday_list, freq='C', tz=pytz.utc)

register('my_bundle_china', china_equities(symbols_china), calendar=calendar)
register('my_bundle_china_pzs', china_equities(symbols_china), calendar=calendar)
