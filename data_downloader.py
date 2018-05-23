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

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
sys.path.append(os.getcwd()) #########
reload(sys)
sys.setdefaultencoding('utf8')

#import set_log  

#log_obj = set_log.Logger('data_downloader.log', set_log.logging.WARNING,
#                         set_log.logging.DEBUG)
#log_obj.cleanup('data_downloader.log', if_cleanup = True)  # 是否需要在每次运行程序前清空Log文件

downloader_args = {
    "start_date": datetime.datetime.strptime(u"2018年5月22日", u"%Y年%m月%d日"), # 获取数据的开始时间
    "end_date": datetime.datetime.strptime(u"2018年5月22日", u"%Y年%m月%d日"), # 获取数据的结束时间
}

mysql_args = {
    "host": "116.62.230.38",
    "user": "spider",
    "password": "startspider",
    "db": "spider",
    "charset": "utf8"
}

class data_downloader(object):

    def __init__(self):
        pass

    def get_data(self):
        sql = "SELECT * FROM `monitor` \
               WHERE `city` = \"浙江土拍网\"  \
               AND DATE(`fixture_date`) BETWEEN DATE('%s') AND DATE('%s')" %(downloader_args["start_date"].strftime("%Y-%m-%d"),\
                                                                             downloader_args["end_date"].strftime("%Y-%m-%d")\
                                                                             )
                                                                                                                       
        with closing(pymysql.connect(host=mysql_args["host"],
                                     user=mysql_args["user"],
                                     password=mysql_args["password"],
                                     db=mysql_args["db"],
                                     charset=mysql_args["charset"]
        )) as conn:
            df = pd.read_sql(sql, conn)

        return df




if __name__ == '__main__':
    data_downloader = data_downloader()
    print(data_downloader.get_data())