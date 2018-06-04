# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: proxy_spider.py
    @time: 2017/3/9 16:27
--------------------------------
"""
import sys
import os

import pandas as pd
import scrapy
import announcements_monitor.items
import re
import traceback
import datetime
import bs4
import numpy as np

log_path = r'%s/log/spider_DEBUG(%s).log' %(os.getcwd(),datetime.datetime.date(datetime.datetime.today()))

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
sys.path.append(os.getcwd()) #########
reload(sys)
sys.setdefaultencoding('utf8')
import spider_log  ########
import spider_func
import time
import requests_manager

requests_manager = requests_manager.requests_manager()
spider_func = spider_func.spider_func()
log_obj = spider_log.spider_log() #########

monitor_page = 5  # 监控目录页数

class Spider(scrapy.Spider):
    name = "500009"

    def start_requests(self):
        global monitor_page
        self.urls = ["http://tdjy.zjdlr.gov.cn/GTJY_ZJ/noticelist_page?SSXZQ=&RESOURCELB=&GDLB=&JYFS=&NOTICENO=&NOTICENAME=&checkNOTICENAME=&startDate=&endDate=&zylb=01&currentPage=%s" %(i+1) for i in range(0, monitor_page)]
        for url in self.urls:
            yield scrapy.Request(url=url, callback=self.catelog_parse, dont_filter=False)

    # def parse(self, response):
    #
    #     for release_time, announcement_url in self.catelog_parse(response.url):
    #         time.sleep(1)
    #         for detail_url in self.announcement_parse(announcement_url):
    #             time.sleep(1)
    #             for ser0, file_url in self.detail_parse(detail_url):
    #                 time.sleep(1)
    #                 ser = ser0.append(pd.Series({"发布时间": release_time}))
    #                 # print(ser)
    #
    #                 # df = df.append(ser, ignore_index=True)
    #                 # df.to_excel("/home/dyson/Desktop/data.xlsx")

    def catelog_parse(self, response):
        try:
            # 解析目录页
            url = response.url
            print(u"目录页：" + url)

            html = requests_manager.get_html(url)
            bs_obj = bs4.BeautifulSoup(html, 'html.parser')
            # with open(u"目录页.html",'w') as f:
                # f.write(bs_obj.prettify())
            e_table = bs_obj.table

            # 解析目录页中的一行数据，即一个公告
            for e_a in e_table.find_all('a'):
                item = announcements_monitor.items.AnnouncementsMonitorItem()
                item['monitor_city'] = '浙江土拍网'

                item['monitor_id'] = self.name
                item['monitor_title'] = e_a.parent.parent.td.get_text(strip=True)

                s = e_a.get('onclick')
                s_list = re.findall("(?<=')\d+?(?=')", s)

                item['monitor_date'] = e_a.parent.find_previous_sibling('td').get_text(strip=True)
                new_url = "http://tdjy.zjdlr.gov.cn/GTJY_ZJ/noticeDetailAction?NOTICEID={0}&GDLB={1}".format(*s_list)

                # yield release_time, new_url
                yield scrapy.Request(url=new_url, meta={'item': item}, callback=self.announcement_parse, dont_filter=False)
        except:
            log_obj.update_error("%s中无法解析\n原因：%s" % (self.name, traceback.format_exc()))

    def announcement_parse(self, response):
        try:
            item = response.meta['item'].copy()
            announcement_url = response.url
            print(u"公告页：" + announcement_url)

            bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
            # with open(u"公告页.html",'w') as f:
                # f.write(bs_obj.prettify())
            e_table = bs_obj.table

            for e_a in e_table.find_all('a'):
                s = e_a.get('href')
                s_list = re.findall("(?<=')\d+?(?=')", s)

                new_url = "http://tdjy.zjdlr.gov.cn/GTJY_ZJ/landinfo?ResourceID={0}&flag={1}".format(*s_list)

                yield scrapy.Request(url=new_url, meta={'item': item}, callback=self.detail_parse, dont_filter=False)
        except:
            log_obj.error(response.url, "%s（ %s ）中无法解析\n%s" % (self.name, response.url, traceback.format_exc()))

    def detail_parse(self, response):
        try:
            detail_url = response.url
            item = response.meta['item'].copy()
            print(u"详情页：" + detail_url)

            bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
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

            item["content_detail"] = pd.DataFrame(ser, columns=[0,]).T
            item["monitor_extra"] = pd.DataFrame({"file_url": file_url, "detail_url": detail_url}, index=[0,])

            item["monitor_title"] = item["monitor_title"] + ser[u"地块编号"]

            b1 = u"拍卖开始时间" in ser and datetime.datetime.strptime(ser[u"拍卖开始时间"], u"%Y年%m月%d日 %H时%M分") > datetime.datetime.now()
            b2 = u"挂牌截止时间" in ser and datetime.datetime.strptime(ser[u"挂牌截止时间"], u"%Y年%m月%d日 %H时%M分") > datetime.datetime.now()
            
            if b1 or b2:
                item["parcel_status"] = "onsell"
            else:
                item["parcel_status"] = "sold"


            yield item
        except:
            log_obj.error(response.url, "%s（ %s ）中无法解析\n%s" % (self.name, response.url, traceback.format_exc()))




if __name__ == '__main__':
    pass
