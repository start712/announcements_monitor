# -*- coding:utf-8 -*-  
#/usr/bin/python3
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: TuPaiWang_update.py
    @time: 18-5-18 下午2:06
--------------------------------
"""
import datetime
import re
import sys
import os
import traceback
from contextlib import closing

import bs4
import pymysql
import pandas as pd
import qq_message

log_path = r'%s/log/spider_DEBUG(%s).log' %(os.getcwd(),datetime.datetime.date(datetime.datetime.today()))

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
sys.path.append(os.getcwd()) #########
reload(sys)
sys.setdefaultencoding('utf8')

import spider_log  ########
log_obj = spider_log.spider_log() #########

import requests_manager
requests_manager = requests_manager.requests_manager()

mysql_args = {
    "host": "116.62.230.38",
    "user": "spider",
    "password": "startspider",
    "db": "spider",
    "charset": "utf8"
}

class TuPaiWang_update(object):

    def __init__(self):
        pass

    def get_data(self):

        global mysql_args

        with closing(pymysql.connect(host=mysql_args["host"],
                                     user=mysql_args["user"],
                                     password=mysql_args["password"],
                                     db=mysql_args["db"],
                                     charset=mysql_args["charset"]
        )) as conn:
            sql = u"SELECT * FROM `monitor` WHERE `city` = \"浙江土拍网\" AND `status` = \"onsell\" ORDER BY insert_time DESC limit 5"
            df = pd.read_sql(sql, conn)

        return df

    def detail_parse(self, detail_url):
        try:
            print(u"详情页：" + detail_url)

            item = {}

            bs_obj = bs4.BeautifulSoup(requests_manager.get_html(detail_url), 'html.parser')
            # with open(u"详情页.html",'w') as f:
            # f.write(bs_obj.prettify())

            file_div = bs_obj.find('div', class_='bt')
            file_id = re.search("(?<=javascript:downLoadDoc\(')\d+?(?='\))", file_div.prettify()).group()
            file_url = "http://tdjy.zjdlr.gov.cn/GTJY_ZJ/downFileAction?rid=%s&fileType=1" % file_id

            e_div = bs_obj.find('div', class_='cotain-box')
            e_table1 = e_div.find('td', class_='font_btn').table
            df1 = pd.read_html(e_table1.prettify(), header=0)[0]

            ser = df1.iloc[0].dropna()

            e_table2 = e_div.find('td', class_='td_line2').table
            df2 = pd.read_html(e_table2.prettify())[0]
            df21 = df2[[0, 1]].dropna(axis=0).set_index([0, ]).T
            df22 = df2[[2, 3]].dropna(axis=0).set_index([2, ]).T

            ser = ser.append(df21.iloc[0]).append(df22.iloc[0])

            item["detail"] = ser.to_json()

            b1 = u"拍卖开始时间" in ser and datetime.datetime.strptime(ser[u"拍卖开始时间"], u"%Y年%m月%d日 %H时%M分") > datetime.datetime.now()
            b2 = u"挂牌起始时间" in ser and datetime.datetime.strptime(ser[u"挂牌起始时间"], u"%Y年%m月%d日 %H时%M分") > datetime.datetime.now()

            if b1 or b2:
                item["status"] = "onsell"
            else:
                item["status"] = "sold"

            return item
        except:
            log_obj.error("%s(%s)中无法解析\n%s" % (u"土拍网数据更新程序", detail_url, traceback.format_exc()))

    def mysql_connect(self, sql, host, user, password, dbname, charset, args=None):
        """
        :return: list
        """
        con = ''
        try:
            con = pymysql.connect(host, user, password, dbname, charset=charset)
            cur = con.cursor()

            # 多条SQL语句的话，循环执行
            if isinstance(sql, list):
                for sql0 in sql:
                    cur.execute(sql0)
            else:
                cur.execute(sql, args)

            data = cur.fetchall()
            con.commit()
            # data.decode('uft8').encode('gbk')
        finally:
            if con:
                # 无论如何，连接记得关闭
                con.close()

        return [list(t) for t in data]

    def update_df_data(self, df, table_name, index_name, host, user, password, dbname, charset):
        sql = "UPDATE %s \n SET " % table_name
        d = df.to_dict()
        # print d
        sql_list = []
        for key in d:
            d0 = d[key]
            l = ["WHEN '%s' THEN '%s'" % (key0, d0[key0]) for key0 in d0]
            sql1 = "`%s` = CASE `%s` \n%s" % (key, index_name, '\n'.join(l))
            sql_list.append(sql1 + '\nEND')
        sql = sql + ',\n'.join(sql_list) + "\nWHERE `%s` IN (%s)" % (index_name, ','.join(["'%s'" % s for s in df.index.tolist()]))
        print sql
        self.mysql_connect(sql, host=host, user=user, password=password, dbname=dbname, charset=charset)

        print("UPDATE successfully !")


    def main(self, name_list):
        # 获取自己数据库中土拍网的在售数据
        df = self.get_data()
        df_old = df.copy()
        
        for r in range(df.shape[0]):
        
            s = df.loc[r, u"extra"]
            ser0 = pd.read_json(s, typ='series')

            item = self.detail_parse(ser0["detail_url"])
            
            # item['used'] = ""
            
            df0 = pd.DataFrame(item, index=[r, ])
            
            df.update(df0)

            # print(df.head(3))

        df = df.set_index(["key",])
        
        # print df

        self.update_df_data(df, "monitor", "key",
                            host=mysql_args["host"],
                            user=mysql_args["user"],
                            password=mysql_args["password"],
                            dbname=mysql_args["db"],
                            charset=mysql_args["charset"]
                            )

        # 编辑QQ消息
        s = u"测试消息\n土拍网挂牌土地监控报告：\n"
        count0 = df["status"].value_counts()

        new_sold = count0["sold"] if "sold" in count0 else new_sold = 0

        s = s + u"距离上次监控，已有%s块挂牌土地成交\n" %new_sold

        ser = df_old["detai"].apply(lambda s:re.search(u"\"拍卖开始时间\":\".+\"|\"挂牌截止时间\":\".+\"", s).group())

        ser = ser.apply(lambda s:re.search(u"\d+年\d+月\d+日", s).group())

        count1 = ser.value_counts()
        date_today = datetime.datetime.now().strftime(u"%Y年%m月%d日")

        new_selling = count1[date_today] if date_today in count1 else new_selling = 0

        s = s + u"今日一共有%s块土地成交" %new_selling










if __name__ == "__main__":
    TuPaiWang_update = TuPaiWang_update()
    TuPaiWang_update.main()
