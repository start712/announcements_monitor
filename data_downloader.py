# -*- coding:utf-8 -*-  
#/usr/bin/python3
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: data_downloader.py
    @time: 18-5-23 下午9:26
--------------------------------
"""
import datetime
import sys
import os
from contextlib import closing
import pymysql
import pandas as pd
import requests
import bs4
import re
import time
import traceback

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
sys.path.append(os.getcwd())
reload(sys)
sys.setdefaultencoding('utf8')

#import set_log  

#log_obj = set_log.Logger('data_downloader.log', set_log.logging.WARNING,
#                         set_log.logging.DEBUG)
#log_obj.cleanup('data_downloader.log', if_cleanup = True)  # 是否需要在每次运行程序前清空Log文件

downloader_args = {
    "start_date": datetime.datetime.strptime(u"2018年5月8日", u"%Y年%m月%d日"), # 获取数据的开始时间
    "end_date": datetime.datetime.strptime(u"2018年5月10日", u"%Y年%m月%d日"), # 获取数据的结束时间
    "if_download_files": False # True为下载,False为不下载,注意大小写
}

mysql_args = {
    "host": "116.62.230.38",
    "user": "spider",
    "password": "startspider",
    "db": "spider",
    "charset": "utf8"
}

headers = {'Accept': '*/*',
            'Accept-Language': 'en-US,en;q=0.8',
            'Cache-Control': 'max-age=0',
            'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/48.0.2564.116 Safari/537.36',
            'Connection': 'keep-alive'
}

class data_downloader(object):

    def __init__(self):
        pass

    def get_data(self):
        sql = "SELECT * FROM `monitor` \
               WHERE `city` = \"浙江土拍网\"  \
               AND DATE(`fixture_date`) BETWEEN DATE('%s') AND DATE('%s')" %(downloader_args["start_date"].strftime("%Y-%m-%d"),
                                                                             downloader_args["end_date"].strftime("%Y-%m-%d")
                                                                             )
                                                                                                                       
        with closing(pymysql.connect(host=mysql_args["host"],
                                     user=mysql_args["user"],
                                     password=mysql_args["password"],
                                     db=mysql_args["db"],
                                     charset=mysql_args["charset"]
        )) as conn:
            df = pd.read_sql(sql, conn)
            
            if df.empty:
                print(u"您选择的日期内没有数据")
                time.sleep(30)
         
        print(u"从数据中读取到表格%s" %df.shape[0])

        return df

    def file_parse(self, file_url, path):
        print("文件下载页：" + file_url)

        global headers
        resp = requests.get(file_url, headers=headers)
        bs_obj = bs4.BeautifulSoup(resp.content, 'html.parser')

        for e_a in bs_obj.find_all('a'):
            s = e_a.get('onclick')
            s_list = re.findall("(?<=')[^,]+?(?=')", s)

            new_url = "http://tdjy.zjdlr.gov.cn/GTJY_ZJ/download?RECORDID={0}&fileName={1}".format(*s_list)
            print("正在下载文件：" + new_url)

            self.get_file(new_url, path + re.sub("[\\\()（）\s]", '', s_list[-1])[:80])

    def get_file(self, url, targetfile):
        global headers
        r = requests.get(url, headers=headers)
        with open(targetfile, "wb") as code:
            code.write(r.content)

        print("====>>>Successfully saving %s" % targetfile)

    def data_unpack(self, df0):

        df0 = df0[["title", "fixture_date", "detail", "extra"]]

        df = pd.DataFrame([])
        for r in range(df0.shape[0]):
            print(u"正在解析%s的数据" % df0.loc[r, "title"].replace("�", ""))
            
            # print df0.loc[r, "detail"]
            ser = pd.read_json(df0.loc[r, "detail"]).loc[0, :]

            df = df.append(ser, ignore_index=True)
            df.loc[r, u"挂牌日期"] = df0.loc[r, "fixture_date"]
            df.loc[r, u"备注"] = df0.loc[r, "extra"]

            
            if downloader_args["if_download_files"]:
                file_url = pd.read_json(df0.loc[r, "extra"]).loc[0, :]["file_url"]
            
                path = os.getcwd() + "\\TuPaiWang_files\\"
                if not os.path.exists(path):
                    os.mkdir(path)
                    
                path = path + re.sub("[\\\()（）\s]", '', df0.loc[r, "title"])[:80] + "\\"
                if not os.path.exists(path):
                    os.mkdir(path)
                
                self.file_parse(file_url, path)
                
            df.to_excel(u"土拍网数据.xlsx")
        



if __name__ == '__main__':
    try:
        data_downloader = data_downloader()
        df0 = data_downloader.get_data()
        data_downloader.data_unpack(df0)
        print(u"\n数据下载完毕")
        time.sleep(60)
        
    except:
        with open(u"土拍网数据下载错误报告.log", "w") as f:
            f.write(traceback.format_exc())
        print(u"\n数据下载出错,已经生成报告")
        time.sleep(60)