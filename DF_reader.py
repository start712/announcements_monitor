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
import time

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

# 标题行存在如下列标题，则更换
title_replace = {
    u'杭州萧山':{u'编号':u'地块编号'},
    u'嘉兴':{u'编号':u'地块编号'},
    u'杭州余杭':{u'挂牌出让地块':u'地块编号'},
    u'金华':{u'宗地':u'地块编号',u'编号':u'地块编号'},
    u'丽水':{u'地块坐落':u'地块名称',u'项目名称':u'地块名称',u'地块位置':u'地块名称',
           u'土地坐落': u'地块名称',u'土地座落':u'地块名称',},
    u'宁波(慈溪)':{u'地块编号':u'地块名称',},
    u'宁波(北仑)':{u'面积（平方米）':u'土地面积',},
    u'宁波(奉化)':{u'出让金额(万元）':u'成交价',},
    u'宁波(象山)':{u'地块':u'地块名称',u'地块编号':u'地块名称',u'宗地编号：':u'地块名称',},
    u'宁波(余姚)':{u'起始价=>单价（万元/亩）':u'单价',u'=>总价（万元）':u'起始价',},
    u'宁波(江北)':{u'出让起始价':u'单价',},
    u'温州':{u'地块编号':u'地块名称',},
    u'舟山':{u'宗地坐落：':u'地块名称',u'地块位置':u'地块名称',u'土地位置':u'地块名称',u'规划用途':u'规划',},
    u'宁波(市局)':{u'起始价（元/平方米）':u'单价'},
    u'台州':{u'用地面积(㎡)':u'备用面积',u'用地面积（㎡）':u'备用面积',u'土地面积(㎡)':u'备用面积',u'土地面积（㎡）':u'备用面积'},
    u'浙江(温州市)':{u'宗地坐落：':u'地块名称',u'地块位置':u'地块名称'},
    u'浙江(舟山市)':{u'宗地坐落：':u'地块名称',u'地块位置':u'地块名称'},
}

# 某一行第一个单元格符合相应正则表达式时，删除整行
abandon_row = {
    u'杭州': [ur'合\s*计',],
    u'金华': [ur'土地使用条件：', ur'备注：'],
    u'嘉兴': [u'嘉兴市资源要素交易中心有限公司',],
    u'丽水': [ur'合\s*计',],
    u'临安': [ur'公示时间',],
    u'衢州': [ur'衢州市国土资源局.+出让结果公布表',ur'合\s*计',],
    u'温州': [ur'合\s*计',],
    u'舟山': [ur'土地使用条件：', ur'备注：'],
    u'宁波(慈溪)':[ur'合\s*计',],
}
# 可能导致正则表达式出错的列
abandon_col = {
    u'杭州':[u'成交价高于起价',],
    u'金华':[u'公告号',],
}
# 由原表中的标题判断
calculated_col = {
    u'宁波(北仑)':{
        u'成交价1':{u'cols':[u'面积（平方米）', u'成交地面地价(元/平方米)'],
                   u'func':np.multiply,
                 },
        u'成交价2':{u'cols':[u'面积（平方米）', u'成交楼面地价(元/平方米)'],
                   u'func':np.multiply,
                 }
    },
    u'宁波(江北)':{
        u'起始价':{u'cols':[u'土地面积(M2)', u'出让起始价'],
                  u'func':lambda x,y: (x*0.0015)*y
                }
    },
    u'宁波(鄞州)': {
        u'成交价1': {u'cols': [u'出让面积(平方米)', u'成交楼面价（元/平方米）'],
                    u'func': np.multiply,
                  },
    },
    u'宁波(市局)':{
        u'起始价':{u'cols':[u'起始价（元/平方米）', u'出让面积（平方米）'],
                  u'func': lambda x,y: x*y/10000}
    },
    u'金华': {
        u'起始价': {u'cols': [u'起始（叫）价(元/ m2)', u'土地面积（m2）'],
                 u'func': lambda x, y: x * y / 10000}
    }
}
# extra_data的date_info中，各个城市读取相应数据的函数
str2extra = {
    u'杭州':{u'sales_deadline': lambda s:re.findall(ur'\d{4}年\d{1,2}月\d{1,2}日',re.search(ur'(挂牌报价|竞买申请)时间.*',s).group())[-1]
                             if re.search(ur'(挂牌报价|竞买申请)时间.*',s) else None,
            u'offer_deadline': lambda s: re.findall(ur'\d{4}年\d{1,2}月\d{1,2}日', re.search(ur'[^(挂牌)]报[名价]时间.*', s).group())[-1]
                             if re.search(ur'[^(挂牌)]报[名价]时间.*', s) else None,
            },
    u'杭州余杭': {u'sales_deadline': lambda s: re.findall(ur'\d{4}年\d{1,2}月\d{1,2}日', re.search(ur'(挂牌报价|竞买申请)时间.*', s).group())[-1]
                                    if re.search(ur'(挂牌报价|竞买申请)时间.*', s) else None,
                 u'offer_deadline': lambda s: re.findall(ur'\d{4}年\d{1,2}月\d{1,2}日', re.search(ur'[^(挂牌)]报[名价]时间.*', s).group())[-1]
                                    if re.search(ur'[^(挂牌)]报[名价]时间.*', s) else None,
                 },
    u'杭州大江东': {u'sales_deadline': lambda s: re.findall(ur'\d{4}年\d{1,2}月\d{1,2}日', re.search(ur'(挂牌报价|竞买申请)时间.*', s).group())[-1]
                                     if re.search(ur'(挂牌报价|竞买申请)时间.*', s) else None,
                   u'offer_deadline': lambda s: re.findall(ur'\d{4}年\d{1,2}月\d{1,2}日', re.search(ur'[^(挂牌)]报[名价]时间.*', s).group())[-1]
                                    if re.search(ur'[^(挂牌)]报[名价]时间.*', s) else None,
                  },
    u'杭州富阳': {u'sales_deadline': lambda s: re.findall(ur'\d{4}年\d{1,2}月\d{1,2}日', re.search(ur'(挂牌报价|拍卖出让)时间.*', s).group())[-1]
                                     if re.search(ur'(挂牌报价|拍卖出让)时间.*', s) else None,
                  u'offer_deadline': lambda s: re.findall(ur'\d{4}年\d{1,2}月\d{1,2}日', re.search(ur'[^(挂牌)]报[名价]时间.*', s).group())[-1]
                                     if re.search(ur'[^(挂牌)]报名(截止)?时间.*', s) else None,
              },

}

# 各个城市中，标题符合相应正则表达式的列，应用公式
unit_standard = {
    u'丽水':{ur'亿元':lambda x:x.astype(np.float64)*1000},
    u'金华':{ur'公顷':lambda x:x.astype(np.float64)*1000},
    u'舟山':{ur'公顷':lambda x:x.astype(np.float64)*1000},
    u'临安':{ur'公顷':lambda x:x.astype(np.float64)*1000,
           ur'挂牌起始价(人民币)':lambda x:x.str.extract('(\d+\.*\d*)', expand=False).astype(np.float64)/10000},
    u'宁波(北仑)':{ur'成交.面地价':lambda x:x.astype(np.float64)/10000},
    u'宁波(江北)':{ur'出让起始价':lambda x:x.str.extract('(\d+\.*\d*)', expand=True)},
    u'宁波(鄞州)':{ur'成交楼面价':lambda x:x.str.extract('(\d+\.*\d*)', expand=False).astype(np.float64)/10000},
    u'宁波(市局)':{ur'起始价（元/平方米）':lambda x:x.str.extract('(\d+\.*\d*)', expand=False)}
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
            elif city == u'宁波':
                if df.loc[r, 'extra']:
                    city0 = pd.read_json(df.loc[r, 'extra']).iloc[0,0]
                    city = '%s(%s)' % (city, city0)
                else:
                    city = '%s(%s)' % (city, '未知')

            l = df.loc[r,'detail'].split('|start|')
            table_info = df.loc[r, ['url', 'title', 'status', 'fixture_date']]
            table_info['city'] = city
            if df.loc[r, 'extra']:
                extra_data = pd.read_json(df.loc[r, 'extra'])
                extra_data.index.name = 'extra_data'
            else:
                extra_data = pd.DataFrame({})
            for s in l:
                df0 = pd.read_json(s,encoding='utf8')
                # 将表格列的顺序还原
                df0.columns = [int(s) for s in df0.columns]
                df0 = df0.sort_index(1)
                df0.index = [int(s) for s in df0.index]
                df0 = df0.sort_index(0)
                # 增加三列便于查找的数据
                #df0['url'],df0['title'],df0['status'] = url,title,status
                yield df0, table_info, extra_data

    def main(self):
        start_time = time.time()
        self.initialization() #删除原有的（test）数据
        data = self.get_data()
        onsell_data = pd.DataFrame([])
        sold_data = pd.DataFrame([])
        update_data = pd.DataFrame([])
        for df, table_info, extra_data in self.originaldata(data):
            originaldata = df.copy()
            city = table_info['city']
            url = table_info['url']
            if re.search(ur'协议|划拨', table_info['title']):
                table_info.to_csv(os.getcwd() + ur'\log\spider_data\☆剔除的数据(data_flow).csv', mode='a',encoding='utf_8_sig')
                df.to_csv(os.getcwd() + ur'\log\spider_data\☆剔除的数据(data_flow).csv', mode='a', encoding='utf_8_sig')
                self.new_row(' \n ' * 2, u'☆剔除的数据')
                continue
            try:
                df = self.output_data(df, table_info, extra_data)
            except:
                log_obj.error('%s,%s' % (city, url))
                log_obj.error(traceback.format_exc())
                table_info.to_csv(os.getcwd() + ur'\log\spider_data\☆问题数据(data_flow).csv', mode='a', encoding='utf_8_sig')
                originaldata.to_csv(os.getcwd() + ur'\log\spider_data\☆问题数据(data_flow).csv', mode='a',encoding='utf_8_sig')
                self.new_row(' \n ' * 2, u'☆问题数据')
                self.new_row(' \n ' * 2, city)
                continue

            try:
                self.new_row(' \n ' * 2, city)

                #df.to_csv(os.getcwd() + r'\log\spider_data\data(data_flow).csv'.decode('utf8'), mode='a', encoding='utf_8_sig')
                if df['status'][0] == 'onsell':
                    onsell_data = onsell_data.append(df)
                elif df['status'][0] == 'sold':
                    sold_data = sold_data.append(df)
                elif df['status'][0] == 'update':
                    update_data = update_data.append(df)
            except:
                log_obj.error('添加数据出错')
                log_obj.error('%s,%s' % (city, url))
                log_obj.error(traceback.format_exc())

        onsell_data.to_csv(os.getcwd() + r'\log\spider_data\onsell_data(data_flow).csv'.decode('utf8'),encoding='utf_8_sig')
        sold_data.to_csv(os.getcwd() + r'\log\spider_data\sold_data(data_flow).csv'.decode('utf8'), encoding='utf_8_sig')
        update_data.to_csv(os.getcwd() + r'\log\spider_data\update_data(data_flow).csv'.decode('utf8'),encoding='utf_8_sig')

        onsell_data = self.remove_duplicated(onsell_data,'onsell_fixture_date')
        sold_data = self.remove_duplicated(sold_data, 'sold_fixture_date')
        df = onsell_data.copy()
        df.update(sold_data)
        df = df.join(sold_data[sold_data.columns[sold_data.columns.isin(onsell_data.columns) == False]], how='left')
        df.to_csv(os.getcwd() + ur'\log\spider_data\☆整合后的数据(data_flow).csv'.decode('utf8'), encoding='utf_8_sig')
        sold_data.loc[sold_data.index.isin(onsell_data.index) == False,:].to_csv(
            os.getcwd() + ur'\log\spider_data\☆无挂牌数据的成交数据(data_flow).csv'.decode('utf8'), encoding='utf_8_sig')

        print "耗时：%smin" %((time.time()-start_time)/60)

    def output_data(self, df, table_info, extra_data):
        original_df = df
        city = table_info['city']
# 输出原始数据
        table_info.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')
        if not extra_data.empty:
            extra_data.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')
        self.new_row('↓↓↓网页数据↓↓↓', city)
        df.fillna("None").to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')
# 修正竖向标题格式的表格
        if city in [ur'金华',ur'宁波(象山)',u'舟山',] or re.search(ur'浙江', city):
            self.new_row(r'修正竖向标题格式的表格↓', city)
            df = data_cleaner.table_standardize(df)
            df.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')

# 删除\xa0空白符
        if city in [u'杭州余杭', u'丽水', u'临安',u'台州']:
            self.new_row(r'删除\xa0空白符↓', city)
            for c in df.columns:
                df[c] = df[c].apply(lambda x:x.replace(u'\xa0','') if isinstance(x,unicode) else x)
            df.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')

# 删除某些特定不需要的行
        if re.search(ur'浙江', city):
            self.new_row('删除包含"%s"的行↓' %','.join([ur'土地使用条件：', ur'备注：']),city)
            df = data_cleaner.row_filter(df,[ur'土地使用条件：', ur'备注：'])
            df.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')
        if city in abandon_row:
            self.new_row('删除包含"%s"的行↓' %','.join(abandon_row[city]),city)
            df = data_cleaner.row_filter(df,abandon_row[city])
            df.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')

# 删除空白行
        if city in [u'杭州富阳',u'嘉兴', u'临安',u'衢州',u'舟山',]:
            self.new_row('删除空白行↓',city)
            if city in [u'杭州富阳', u'衢州',u'舟山',u'嘉兴']: df = data_cleaner.blank_row_cleaner(df,num=1)
            if city == u'临安': df = data_cleaner.blank_row_cleaner(df)
            df.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')

# 第一行数据添加为列标题，增加原始数据列
        self.new_row('第一行数据添加为列标题，增加原始数据列↓', city)
        df = data_cleaner.title_standardize(df, fillna_method=None)  # 第一行数据添加为列标题
# 整合额外数据
        if city in [u'丽水',] and not extra_data.empty:
            self.new_row('整合额外数据↓', city)
            df = df.join(extra_data.iloc[:,np.where(extra_data.columns.isin(df.columns)==False)[0]], how='left')

        df = data_cleaner.original_data(df)  # 增加原始数据列
        df.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')

# 统一单位
        if city in unit_standard:
            self.new_row('根据原标题统一单位↓', city)
            for s in unit_standard[city]:
                # 出现TypeError: 'numpy.int64' object is not iterable，extract中的参数expand改成False
                df0 = df[filter(lambda x: re.search(s, x), df.columns)].apply(unit_standard[city][s])
                df.update(df0)
            df.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')
        if re.search(ur'浙江', city):
            self.new_row('将单位公顷更改为平方米↓', city)
            df0 = df[filter(lambda x: re.search(ur'公顷', x), df.columns)].apply(lambda x:x.astype(np.float64)*1000)
            df.update(df0)
            df.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')

# 增加新列
        if city in calculated_col:
            for key in calculated_col[city]:
                info = calculated_col[city][key]
                if np.all(pd.DataFrame(info[u'cols']).isin(df.columns)):
                    self.new_row('增加新列=>%s↓' % key, city)
                    df0 = df[info[u'cols']]
                    df[key] = reduce(info[u'func'],[df0[s].astype(np.float64) for s in df0.columns])
                df.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')

# 更换一些奇葩的标题
        if city in title_replace:
            self.new_row('更换一些奇葩的标题↓', city)
            #df.columns = [s.strip() for s in df.columns]
            df = df.rename(columns=title_replace[city])
            df.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')

# 删除会导致正则表达式出错的列
        if city in abandon_col:
            self.new_row('删除会导致正则表达式出错的列"%s"↓' %','.join(abandon_col[city]), city)
            df = data_cleaner.col_filter(df,abandon_col[city])
            df.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')

#提取所需数据,并修改列标题
        self.new_row('提取所需数据,并修改列标题↓', city)
        df = data_cleaner.data_extract(df) # 提取所需数据
        df.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')

# 修正个别列的数据
        self.new_row('修正个别列的数据↓', city)
        if re.search(ur'浙江', city):
            df = data_cleaner.col_format(df, 'starting_price_sum', data_cleaner.num_picker_first)
            df = data_cleaner.col_format(df, 'offer_area_m2', data_cleaner.num_picker_first)
        if city in [u'湖州', u'临安',u'衢州', u'宁波(象山)',u'台州']:
            df = data_cleaner.col_format(df, 'starting_price_sum', data_cleaner.num_picker_first)
        if city in [u'丽水', u'宁波(象山)', u'衢州',u'金华',u'舟山',u'温州',]:
            df = data_cleaner.col_format(df, 'offer_area_m2', data_cleaner.num_picker_first)
        if city in [u'宁波(镇海)']:
            df = data_cleaner.col_format(df, 'parcel_name', lambda x:re.sub(ur'地块$|\s+','',x))
        df = data_cleaner.col_format(df, 'parcel_no', data_cleaner.parcel_no_cleaner)
        df = data_cleaner.col_format(df, 'plot_ratio', data_cleaner.plot_ratio_cleaner)
        df = data_cleaner.col_format(df, 'building_area', data_cleaner.building_area_cleaner)
        df = data_cleaner.col_format(df, 'transaction_price_sum', data_cleaner.num_picker_max)
        df.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')

# 整理数据行中的合并单元格
        if city in [u'杭州',u'杭州富阳',u'嘉兴', u'金华', u'丽水', u'宁波',u'绍兴']:
            self.new_row('整理数据行中的合并单元格↓', city)
            df = data_cleaner.data_standardize(df) # 整理数据行中的合并单元格
            df.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')

# 添加网页里的其他相关数据
        if city in str2extra and not extra_data.empty and 'date_info' in extra_data.columns:
            self.new_row('添加网页里的其他相关数据↓', city)
            for key in str2extra[city]:
                func = str2extra[city][key]
                df[key] = func(extra_data.loc[0,'date_info'])

# 设置parcel_key
        self.new_row('设置地块唯一标识，并设置为行标题↓', city)

        if city in [u'宁波(余姚)',u'浙江(宁波市)',u'浙江(绍兴市)'] or re.search(u'杭州|湖州|嘉兴', city):
            self.new_row('地块编号（md5）↓', city)
            df.index = city + df['parcel_no'].apply(data_cleaner.str2md5)
            df['original_key'] = city + df['parcel_no']
        elif re.search(u'金华', city):
            if table_info['status'] == 'onsell' and city == u'金华':
                self.new_row('标题+地块位置+地块面积（md5）↓', city) #offer_area_m2
                df.index = city + (table_info['title'] + df['parcel_location'] + df['offer_area_m2']).apply(data_cleaner.str2md5)
                df['original_key'] = city + (table_info['title'] + df['parcel_location'] + df['offer_area_m2'])
            else:
                self.new_row('标题中地块编号(或者告字号)+地块位置+地块面积（md5）↓', city)
                title = table_info['title']
                title = re.sub(ur'（', '\(', title)
                title = re.sub(ur'）', '\)', title)
                parcel_no0 = re.search(ur'(?<=\().+(?=\))', title).group()
                df.index = city + (parcel_no0 + df['parcel_location'] + df['offer_area_m2']).apply(data_cleaner.str2md5)
                df['original_key'] = city + (parcel_no0 + df['parcel_location'] + df['offer_area_m2'])
        elif re.search(ur'丽水', city):
            self.new_row('告字号 + 地块面积（md5）↓', city)
            m = re.search(ur'丽.+号', table_info['title'])
            if m:
                title = m.group()
            else:
                title = re.search(u'(?<=[\(（]).+(?=[\)）])', table_info['title']).group()
            df.index = city + (title + df['offer_area_m2'].astype(np.str)).apply(data_cleaner.str2md5)
            df['original_key'] = city + (title + df['offer_area_m2'].astype(np.str))
        elif city in [u'临安',]:
            self.new_row('标题+地块位置（md5）↓', city)
            df.index = city + (table_info['title'] + df['parcel_location']).apply(data_cleaner.str2md5)
            df['original_key'] = city + (table_info['title'] + df['parcel_location'])
        elif city in [u'宁波(北仑)',u'宁波(慈溪)',u'宁波(奉化)',u'宁波(市局)',u'宁波(象山)',u'宁波(镇海)',u'绍兴',
                      u'温州',u'宁波(江北)',u'舟山',u'宁波(鄞州)',u'浙江(温州市)',u'浙江(舟山市)',]:
            self.new_row('地块名称（md5）↓', city)
            df.index = city + (df['parcel_name']).apply(data_cleaner.str2md5)
            df['original_key'] = city + df['parcel_name']
        elif city in [u'衢州',u'台州',u'浙江(台州市)']:
            self.new_row('地块位置（md5）↓', city)
            df.index = city + (df['parcel_location']).apply(data_cleaner.str2md5)
            df['original_key'] = city + (df['parcel_location'])

        df.index.name = 'parcel_key'

        df.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')

# 特殊处理
        if city in [u'舟山',]:
            self.new_row('特殊处理↓', city)
            df = data_cleaner.special_process(df,city)
            df.to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a', encoding='utf_8_sig')

# 将个别列的onsell和sold数据分开
        change_list = ['url','fixture_date']
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
        pd.Series(['-' * 150, ], index=['',]).to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a',
                                        encoding='utf_8_sig')
        pd.Series([s, ], index=['',]).to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a',
                                        encoding='utf_8_sig')
        pd.Series(['-' * 150, ], index=['',]).to_csv(os.getcwd() + r'\log\spider_data\%s(data_flow).csv' % city, mode='a',
                                        encoding='utf_8_sig')
    def initialization(self):
        path = r'C:\Users\lenovo\Desktop\Projects\PythonProgramming\announcements_monitor\log\spider_data'
        for t in os.walk(path):
            l = t[2]
        l = filter(lambda x:re.search(r'\(data_flow\)',x), l)
        for s in l:
            os.remove(path + r'\\' + s)

    def remove_duplicated(self,df, col):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col])
            df = df.sort_values(col, ascending=True)

            b = df.index.duplicated()
            # 重复项存入csv，返回不重复的
            self.new_row('以下是%s中的重复数据↓' %col, u'☆重复的数据')
            df.loc[b == True, :].to_csv(os.getcwd() + ur'\log\spider_data\☆重复的数据(data_flow).csv', mode='a', encoding='utf_8_sig')
            return df.loc[b==False,:]


if __name__ == '__main__':
    DF_reader = DF_reader()
    #data = DF_reader.get_data()
    #print DF_reader.get_data()
    #DF_reader.originaldata2csv(data)
    #for df in DF_reader.originaldata(data):
    #    print df
    DF_reader.main()