# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: DF_reader.py
    @time: 2017/8/1 11:42
--------------------------------
"""
import sys
import os
import traceback
from contextlib import closing
import pymysql
import pandas as pd
import json
import numpy as np

import re

import time

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import set_log  # log_obj.debug(文本)  "\x1B[1;32;41m (文本)\x1B[0m"
import mysql_connecter
import data_cleaner
data_cleaner = data_cleaner.data_cleaner()
mysql_connecter = mysql_connecter.mysql_connecter()
log_obj = set_log.Logger('DF_reader.log', set_log.logging.WARNING,
                         set_log.logging.DEBUG)
log_obj.cleanup('DF_reader.log', if_cleanup=True)  # 是否需要在每次运行程序前清空Log文件

with open('states_belonging.json') as f:
    states_belonging = json.load(f, encoding='utf8')

city_list = [u'杭州',u'宁波',u'绍兴',u'湖州',u'嘉兴',u'金华',u'衢州',u'台州',u'丽水',u'舟山']

# 标题行中包含如下字符串，则更换标题
title_replace = {
    u'杭州萧山':{u'编号':u'地块编号'},
    u'嘉兴':{u'编号':u'地块编号'},
    u'杭州余杭':{u'挂牌出让地块':u'地块编号'},
    u'金华':{u'宗地':u'地块编号',u'编号':u'地块编号'},
}
# 某一行第一个单元格包含以下内容时，删除整行
abandon_row = {
    u'杭州': [ur'合\s*计',],
    u'金华': [ur'土地使用条件：', ur'备注：'],
    u'嘉兴': [u'嘉兴市资源要素交易中心有限公司',],
}
# 可能导致正则表达式出错的列
abandon_col = {
    u'杭州':[u'成交价高于起价',],
    u'金华':[u'公告号',],
}

class DF_reader(object):
    def __init__(self):
        pass
    def get_data(self):
        sql = "SELECT * FROM `monitor` WHERE detail <> ''"
        with closing(pymysql.connect(host='localhost', user='spider',password='startspider',
                                     database='spider', charset='utf8')) as conn:
            df = pd.read_sql(sql, conn)
        return df

    def originaldata(self, df):
        for r in xrange(len(df.index)):
            city = df.loc[r,'city']
            if city == u'浙江':
                title = df.loc[r, 'title']
                m = re.search(ur'.{2}市|.{2,6}县', title)
                if m:
                    city0 = m.group()
                    if city0 in states_belonging:
                        city = '%s(%s)' %(city, states_belonging[city0])
                    elif city0 in states_belonging.values():
                        city = '%s(%s)' %(city, city0)
                    else:
                        city = '%s(-?-%s-?-)' %(city,city0)
                else:
                    city = '%s(%s)' %(city,'未知')
            l = df.loc[r,'detail'].split('|start|')
            table_info = df.loc[r, ['url', 'title', 'status', 'fixture_date']]
            table_info['city'] = city
            extra_data = df.loc[r, ['extra']]
            for s in l:
                df0 = pd.read_json(s)
                # 将表格列的顺序还原
                df0.columns = [int(s) for s in df0.columns]
                df0 = df0.sort_index(1)
                df0.index = [int(s) for s in df0.index]
                df0 = df0.sort_index(0)
                # 增加三列便于查找的数据
                #df0['url'],df0['title'],df0['status'] = url,title,status
                yield df0, table_info, extra_data

    def main(self):
        self.initialization() #删除原有的（test）数据
        data = self.get_data()
        onsell_data = pd.DataFrame([])
        sold_data = pd.DataFrame([])
        update_data = pd.DataFrame([])
        for df, table_info, extra_data in self.originaldata(data):
            city = table_info['city']
            url = table_info['url']
            try:
                df = self.output_data(df, table_info, extra_data)

                """
                if df['city'][0] not in [u'杭州', u'杭州大江东', u'杭州富阳', u'杭州萧山', u'杭州余杭', u'湖州']:
                    continue

                if df['status'][0] == 'onsell':
                    onsell_data = onsell_data.append(df)
                elif df['status'][0] == 'sold':
                    sold_data = sold_data.append(df)
                elif df['status'][0] == 'update':
                    update_data = update_data.append(df)

                onsell_data.to_csv(os.getcwd() + r'\log\spider_data\onsell_data(data_flow).csv'.decode('utf8'),encoding='utf_8_sig')
                sold_data.to_csv(os.getcwd() + r'\log\spider_data\sold_data(data_flow).csv'.decode('utf8'), encoding='utf_8_sig')
                update_data.to_csv(os.getcwd() + r'\log\spider_data\update_data(data_flow).csv'.decode('utf8'),encoding='utf_8_sig')
                df = onsell_data
                df.update(sold_data)
                df = df.join(sold_data[sold_data.columns[sold_data.columns.isin(onsell_data.columns) == False]], how='left')
                df.to_csv(os.getcwd() + r'\log\spider_data\df(data_flow).csv'.decode('utf8'), encoding='utf_8_sig')
                onsell_data[onsell_data.columns[onsell_data.columns.isin(sold_data.columns) == False]].to_csv(
                    os.getcwd() + r'\log\spider_data\匹配不到待售数据的已售数据(data_flow).csv'.decode('utf8'), encoding='utf_8_sig')
                """
            except:
                log_obj.error('%s,%s' %(city,url))
                log_obj.error(traceback.format_exc())

            self.new_row('-' * 150, city)
            self.new_row('', city)
            self.new_row('', city)

    def output_data(self, df, table_info, extra_data):
        original_df = df
        city = table_info['city']
# 输出原始数据
        table_info.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')
        self.new_row('网页数据↓', city)
        df.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')
        self.new_row('-' * 150, city)

# 删除\xa0空白符
        if city in [u'杭州余杭', u'丽水']:
            self.new_row(r'删除\xa0空白符↓', city)
            for c in df.columns:
                df[c] = df[c].apply(lambda x:x.replace(u'\xa0','' if isinstance(x,unicode) else x))
            df.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')
            self.new_row('-' * 150, city)

# 删除某些特定不需要的行
        if city in abandon_row:
            self.new_row('删除包含%s的行↓' %','.join(abandon_row[city]),city)
            df = data_cleaner.row_filter(df,abandon_row[city])
            df.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')
            self.new_row('-' * 150, city)

# 删除空白行
        if city in [u'杭州富阳',u'嘉兴']:
            self.new_row('删除空白行↓',city)
            if city == u'杭州富阳': df = data_cleaner.blank_row_cleaner(df,num=1)
            if city == u'嘉兴': df = data_cleaner.blank_row_cleaner(df, row=0, num=1)
            df.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')
            self.new_row('-' * 150, city)


# 第一行数据添加为列标题，增加原始数据列
        self.new_row('第一行数据添加为列标题，增加原始数据列↓', city)
        df = data_cleaner.title_standardize(df, fillna_method=None)  # 第一行数据添加为列标题
# 更换一些奇葩的标题
        if city in title_replace:
            self.new_row('更换一些奇葩的标题↓', city)
            #print df.columns
            #print city, title_replace[city]
            df = df.rename(columns=title_replace[city])
        df = data_cleaner.original_data(df) # 增加原始数据列
        df.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')
        self.new_row('-' * 150, city)

# 统一单位
        if city in [u'金华',]:
            self.new_row('统一单位↓', city)
            df.update(df[filter(lambda x:re.search(ur'公顷',x), df.columns)].apply(lambda x:x*1000))
            df.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')
            self.new_row('-' * 150, city)

# 删除会导致正则表达式出错的列
        if city in abandon_col:
            self.new_row('删除会导致正则表达式出错的列%s↓' %','.join(abandon_col[city]), city)
            df = data_cleaner.col_filter(df,abandon_col[city])
            df.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')
            self.new_row('-' * 150, city)

#提取所需数据,并修改列标题
        self.new_row('提取所需数据,并修改列标题↓', city)
        df = data_cleaner.data_extract(df) # 提取所需数据
        df.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')
        self.new_row('-' * 150, city)

# 修正个别列的数据
        self.new_row('修正个别列的数据↓', city)
        if city in [u'湖州', ]:
            df = data_cleaner.col_format(df, 'starting_price_sum', data_cleaner.num_picker_first)
        if city in [u'丽水', ]:
            df = data_cleaner.col_format(df, 'offer_area_m2', data_cleaner.num_picker_first)
        df = data_cleaner.col_format(df, 'parcel_no', data_cleaner.parcel_no_cleaner)
        df = data_cleaner.col_format(df, 'plot_ratio', data_cleaner.plot_ratio_cleaner)
        df = data_cleaner.col_format(df, 'building_area', data_cleaner.building_area_cleaner)
        self.new_row('-' * 150, city)
        df = data_cleaner.col_format(df, 'transaction_price_sum', data_cleaner.num_picker_max)
        df.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')
        self.new_row('-' * 150, city)

# 整理数据行中的合并单元格
        if city in [u'杭州',u'杭州富阳',u'嘉兴', u'金华']:
            self.new_row('整理数据行中的合并单元格↓', city)
            df = data_cleaner.data_standardize(df) # 整理数据行中的合并单元格
            df.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')
            self.new_row('-' * 150, city)

# 设置parcel_key
        self.new_row('设置地块唯一标识，并设置为行标题↓', city)
        try:
            if city in [u'杭州', u'杭州大江东', u'杭州富阳', u'杭州萧山', u'杭州余杭', u'湖州', u'嘉兴']:
                df.index = city + df['parcel_no'].apply(data_cleaner.str2md5)
                df.index.name = 'parcel_key'
            elif city in [u'金华']:
                if table_info['status'] == 'sold':
                    title = table_info['title']
                    title = re.sub(ur'（', '\(', title)
                    title = re.sub(ur'）', '\)', title)
                    parcel_no0 = re.search(ur'(?<=\().+(?=\))', title).group()
                    df.index = city + (parcel_no0 + df['parcel_location']).apply(data_cleaner.str2md5)
                    df.index.name = 'parcel_key'
                else:
                    df.index = city + (table_info['title'] + df['parcel_location']).apply(data_cleaner.str2md5)
                    df.index.name = 'parcel_key'
        except:
            self.new_row('赋值parcel_key出错', city)
            self.new_row('赋值parcel_key出错', city)
            self.new_row('赋值parcel_key出错', city)
            print city,table_info['url']
            print traceback.format_exc()
            table_info.to_csv(os.getcwd() + ur'\log\spider_data\问题数据(data_flow).csv', mode='a', encoding='utf_8_sig')
            original_df.to_csv(os.getcwd() + ur'\log\spider_data\问题数据(data_flow).csv', mode='a', encoding='utf_8_sig')

        df.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')

# 将个别列的onsell和sold数据分开
        change_list = ['url',]
        for title in table_info.index:
            if title in change_list:
                df[table_info['status'] + '_' + title] = table_info[title]
            else:
                df[title] = table_info[title]
        return df

    def originaldata2csv(self, df):
        for r in xrange(len(df.index)):
            city = df.loc[r,'city']
            if city == u'浙江':
                title = df.loc[r, 'title']
                #print
                #print title
                m = re.search(ur'.{2}市|.{2,6}县', title)
                if m:
                    city0 = m.group()
                    #print city0
                    if city0 in states_belonging:
                        city = '%s(%s)' %(city, states_belonging[city0])
                    elif city0 in states_belonging.values():
                        city = '%s(%s)' %(city, city0)
                    else:
                        city = '%s(-?-%s-?-)' %(city,city0)
                else:
                    city = '%s(%s)' %(city,'未知')
                #print city
            df.loc[r,['url','city','title','status','extra']].to_csv(os.getcwd()+r'\log\spider_data\%s.csv' %city, mode='a', encoding='utf_8_sig')
            l = df.loc[r,'detail'].split('|start|')
            #print df.loc[r,'url']
            for s in l:
                df0 = pd.read_json(s)
                # 将表格列的顺序还原
                df0.columns = [int(s) for s in df0.columns]
                df0 = df0.sort_index(1)
                df0.index = [int(s) for s in df0.index]
                df0 = df0.sort_index(0)

                df0.to_csv(os.getcwd()+r'\log\spider_data\%s.csv' %city, mode='a', encoding='utf_8_sig')
            pd.Series(['-'*60,]).to_csv(os.getcwd()+r'\log\spider_data\%s.csv' %city, mode='a', encoding='utf_8_sig')

    def new_row(self, s, city):
        pd.Series([s, ]).to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a',
                                        encoding='utf_8_sig')
    def initialization(self):
        path = r'C:\Users\lenovo\Desktop\Projects\PythonProgramming\announcements_monitor\log\spider_data'
        for t in os.walk(path):
            l = t[2]
        l = filter(lambda x:re.search(r'\(data_flow\)',x), l)
        for s in l:
            os.remove(path + r'\\' + s)

if __name__ == '__main__':
    DF_reader = DF_reader()
    #data = DF_reader.get_data()
    #print DF_reader.get_data()
    #DF_reader.originaldata2csv(data)
    #for df in DF_reader.originaldata(data):
    #    print df
    DF_reader.main()