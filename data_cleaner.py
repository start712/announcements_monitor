# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: data_cleaner.py
    @time: 2017/8/4 11:23
--------------------------------
"""
import json
import sys
import os
import re
import numpy as np
import pandas as pd
import hashlib

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')
import set_log
log_obj = set_log.Logger('data_cleaner.log', set_log.logging.WARNING,
                         set_log.logging.DEBUG)
log_obj.cleanup('data_cleaner.log', if_cleanup=True)  # 是否需要在每次运行程序前清空Log文件

float_col = [u'offer_area_m2', u'building_area', u'starting_price_sum', u'transaction_price_sum', ] # 需要计算的列

class data_cleaner(object):
    def __init__(self):
        pass

    def title_fix(self,df,key_word=None):
        if key_word:
            m = re.search(key_word,df.iloc[0,0])
            if m:
                df = df.drop([0, ], axis=0)
                df.index = range(len(df.index))  # 索引重新从0计算
        return df

    def title_standardize(self, df, delimiter='=>', b0 = True, fillna_method='ffill'):
        # 若第一列从第二个单元格之后全是空白，则肯定不是标题栏，所以删除
        if not np.any(np.array(df.iloc[0,:][1:])):
            df = df.drop([0,],axis=0)
            df.index = range(len(df.index))  # 索引重新从0计算

        """将数据的标题与数据分离，将有合并单元的行合并"""
        if b0 and df.iloc[0,:].hasnans and df.iloc[1,:].hasnans:# 假设第一排数据行没有横向合并单元格
            if fillna_method:
                df.iloc[0, :] = df.iloc[0, :].fillna(method=fillna_method) + (delimiter + df.iloc[1,:]).fillna('')
            else:
                df.iloc[0, :] = df.iloc[0, :].fillna('') + (delimiter + df.iloc[1,:]).fillna('')
            df = df.drop([1,], axis=0)

        df.columns = df.iloc[0,:]
        df.columns.name = None
        df = df.drop([0,], axis=0)

        df.index = range(len(df.index)) # 索引重新从0计算
        return df

    def col_filter(self, df, abandon_col):
        # 删除会导致正则表达式出错的列
        b0 = [filter(lambda x:re.search(x,s).group() if isinstance(s,unicode) and re.search(x,s) else None, abandon_col) for s in df.columns]
        df = df.drop(df.columns[np.where(b0)[0]], axis=1)
        return df

    def row_filter(self, df, abandon_row):
        # 删除会导致正则表达式出错的行
        #b = df.iloc[:,0].apply(lambda x:re.search(r,x) != None if isinstance(x,unicode) else False)
        b0 = [filter(lambda x:re.search(x,s).group() if isinstance(s,unicode) and re.search(x,s) else None, abandon_row) for s in df.iloc[:,0]]
        df = df.drop(df.index[np.where(b0)[0]], axis=0)
        df.index = range(df.shape[0])
        return df   #.drop(b[b==True].index,axis=0)

    def data_extract(self, df):
        # 将需要用的列选出来并同步为数据库中的名字
        with open('title_re.json') as f:
            title_re = json.load(f, encoding='utf8')

        # 找出需要的列，并统一成正确的列名
        b = [filter(lambda x:re.search(x,s).group() if isinstance(s,unicode) and re.search(x,s) else None, title_re) for s in df.columns]
        arr = np.array(df.columns)[np.where(b)[0]]
        df = df[arr]
        s_arr = np.array(b)[np.where(b)[0]]
        #print len(df.columns),len(s_arr),len(arr)
        df.columns = [title_re[l[0]] for l in s_arr]
        return df

    def data_standardize(self, df, delimiter0='|'):
        for r in xrange(df.shape[0]-1, -1, -1):
            if df.iloc[r,:].hasnans:
                delimiter = df.iloc[r-1, :].apply(
                    lambda x:0 if isinstance(x,np.int64) or isinstance(x,np.float64) else delimiter0
                )
                #print df.iloc[r-1, :]
                #print (delimiter + df.iloc[r, :]).fillna('')
                df.iloc[r-1, :] = df.iloc[r-1, :] + (delimiter + df.iloc[r, :]).fillna('')
                df = df.drop(r,axis=0)
        df.index = range(len(df.index))  # 索引重新从0计算
        return df

    def table_standardize(self, df, col_count=6):
        # 表格宽度为偶数列，且第一列的每个单元格中都有：或:
        fc = df[0]
        b = fc.apply(lambda x:str(x) if x else ur'：:')
        b = b.apply(lambda x:True if (isinstance(x,unicode) or isinstance(x,str)) and
                                     (re.search(r'：|:', x) or re.search(ur'：|:', x)) else False)
        if False in b.value_counts().to_dict():
            return df
        else:
            arr = np.array(df).reshape(-1,2)
            df = pd.DataFrame(arr)
            #print df
            # 删除空白列
            df = self.blank_row_cleaner(df)
            return df.T

    def blank_row_cleaner(self, df, row='all', num=0, method='<='):
        """
        :param row: 规定检验哪一行，all则全部检验
        :param num: 忽略几个不是None的单元格
        :param method: 检验特定的某一行时候，忽略的单元格数量<=num的都行，还是一定要=num才删除
        :return:
        """
        # 删除指定行，非空白
        if row=='all':
            sers = [df.iloc[r,:].apply(lambda x:x if x else None) for r in xrange(df.shape[0])]
            # 列表中每个元素都是一个字典，包含原数据每一行中有几个True（空白）
            b = [ser.isnull().value_counts().to_dict() for ser in sers]
            b = [b0[True] if True in b0 else 0 for b0 in b] #显示TRUE的个数，没有则为0
            r = [i for i in xrange(len(b)) if b[i] >= df.shape[1] - num] # 哪些行符合条件
            df = df.drop(r, axis=0)
        else:
            ser = df.iloc[row, :].apply(lambda x:x if x else None)
            b = ser.isnull().value_counts().to_dict()
            if True in b:
                none_count = b[True]
                if method == '<=' and none_count >= df.shape[1] - num:
                    df = df.drop([row,], axis=0)
                elif none_count == df.shape[1] - num:
                    df = df.drop([row, ], axis=0)
        df.index = range(len(df.index))  # 索引重新从0计算
        return df

    def original_data(self, df):
        for r in df.index:
            try:
                original_data = df.loc[r,:].to_json(force_ascii=False)
                df.loc[r, 'original_data'] = original_data
            except:
                df.loc[r, 'original_data'] = 'wrong_data_format'
        return df

    def str2md5(self, s):
        s = str(s)
        m = hashlib.md5()
        m.update(s)
        return m.hexdigest()

    def col_format(self, df, col, func):
        if col in df.columns:
            df[col] = df[col].apply(func)
            df[col] = df[col][df[col].isnull() == False].apply(lambda x: str(x))
        return df

    def num_picker_first(self, s):
        # 选取一个字符串中第一个带小数点的数字
        if isinstance(s, unicode):
            m = re.search(ur'\d+[\.]*\d*',s)
            if m:
                return m.group()
        else:
            return s

    def num_picker_max(self, s):
        # 选取一个字符串中第一个带小数点的数字
        if isinstance(s, unicode):
            m = re.search(ur'\d+[\.]*\d*',s)
            if m:
                return max(re.findall(ur'\d+[\.]*\d*',s))
        else:
            return s

    def parcel_no_cleaner(self, s):
        #print s, '------', type(s.decode('utf8')), '--', type(u'')
        s = str(s)
        if type(s) == type(u''):
            r1 = re.compile(ur'[【〔\(（]+')
            r2 = re.compile(ur'[】〕\)）]+')
            s = r1.sub('[', s)
            s = r2.sub(']', s)
        m = re.search(ur'地块$', s)
        if m:
            s = re.sub(m.group(),'',s)
        return s

    def plot_ratio_cleaner(self, s):
        if not s:
            return s
        res = -100
        s = str(s)
        # (1)搜索所有带百分号的数字，然后返回最大值除以100
        comp = re.compile(r'\d+(?=%)')
        if comp.search(s):
            res = float(max(comp.findall(s)))/100
        # (2)搜索【数字+“号”字】的模式，返回空白
        comp = re.compile(r'\d+号')
        if comp.search(s):
            return ''
        # (3)搜索所有可能带小数点的数字，以数组形式返回，若数组中数字个数大于2个，
        # 返回空白，否则返回最大的那个数字
        comp = re.compile(r'\d+\.*\d*')
        if comp.search(s):
            if len(comp.findall(s)) > 2:
                return ''
            else:
                res = max(comp.findall(s))
        # (4)若有结果，但是结果大于10，返回空白
        res = float(res)
        if res < 0 or res > 10:
            return ''
        return res

    def building_area_cleaner(self, s):
        if not s:
            return s
        s = str(s)
        # (1)搜索带有地上面积字符串
        m = re.search(ur'(?<=地上).*?\d+[\.]*\d*', s)
        if m:
            return re.search(ur'\d+[\.]*\d*', m.group()).group()
        # (2)搜索所有带小数点的数字，返回最大值
        if re.search(u'\d+[\.]*\d*', s):
            return float(max(re.findall(u'\d+[\.]*\d*', s)))
        else:
            return s

if __name__ == '__main__':
    pass