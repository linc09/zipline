# coding=utf-8

"""
strategy helper
"""

import pandas as pd


def generate_group(group_num, hs300_weight_pd, stock_industry_pd, stock_factor_pd):
    """
    生成股票投资分组，划分为group_num个组，每组内行业的投资权重与沪深300相同
    :param group_num: 分组数
    :param hs300_weight_pd: 沪深300股票权重, hs300_weight = ts.get_hs300s()
    :param stock_industry_pd: 股票行业列表, FactorAccessTCsv.get_factor_values_all('stock_industry_shenwan_no1')
    :param stock_factor_pd: 股票因子列表, fa.get_factor_values_all('fea00015', '2016-06-06', '2016-06-06')
    """
    # 计算沪深300行业权重
    hs300_indust_weight = dict()
    for i in hs300_weight_pd.index:
        stock_code = hs300_weight_pd.loc[i, 'code']
        stock_weight = hs300_weight_pd.loc[i, 'weight']
        stock_indust = stock_industry_pd[stock_industry_pd.stock_code==stock_code]['industry_name'].values[0]
        if stock_indust not in hs300_indust_weight.keys():
            hs300_indust_weight[stock_indust] = 0.0
        hs300_indust_weight[stock_indust] += stock_weight

    # 根据因子和沪深300行业权重分组
    stock_indust_factor_pd = pd.merge(
        left=stock_industry_pd, right=stock_factor_pd, how='left', left_on='stock_code', right_on='stock_code'
    ).dropna(how='any')
    # 保持分组结果{ 分组1:[(股票1, 权重1), (股票2, 权重2), ...], 分组2:...}
    invest_group_dict = dict()
    for i in range(1, group_num+1):
        invest_group_dict[i] = dict()
        for indust_name in hs300_indust_weight.keys():
            one_indust = stock_indust_factor_pd[stock_indust_factor_pd.industry_name == indust_name]
            one_indust['factor_value'] = one_indust['factor_value'].astype(float)
            one_indust = one_indust.sort_values(by='factor_value', ascending=False)
            # 每组当前行业的股票数
            group_indust_stock_num = int(len(one_indust) / group_num)
            # 每组当前行业股票的投资权重
            stock_weight = hs300_indust_weight[indust_name] / group_indust_stock_num
            for j in range((i-1)*group_indust_stock_num, i*group_indust_stock_num):
                invest_group_dict[i][one_indust.iloc[j]['stock_code']] = stock_weight

    return invest_group_dict


def cancal_all_open_orders():
    """
    根据zipline的get_open_orders返回值取消其所有订单
    """
    import zipline.api
    import pandas.tslib.Timestamp



if __name__ == '__main__':
    import tushare as ts
    from jingong_factor_access.get_factor import FactorAccessTCsv, FactorAccessTMysql
    # 股票所属行业
    stock_ind = FactorAccessTCsv.get_factor_values_all('stock_industry_shenwan_no1')
    # 沪深300权重股
    hs300_weight = ts.get_hs300s()
    # 股票因子
    fa = FactorAccessTMysql()
    stock_factor = fa.get_factor_values_all('fea00015', '2016-06-06', '2016-06-06')
    generate_group(5, hs300_weight, stock_ind, stock_factor)
    pass
    import pylab as plt
