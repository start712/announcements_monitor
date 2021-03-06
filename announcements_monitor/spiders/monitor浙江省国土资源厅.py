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
import traceback
import bs4
import numpy
import requests
import scrapy
import announcements_monitor.items
import re
import datetime
import pandas as pd
import json

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
sys.path.append(os.getcwd()) #########
reload(sys)
sys.setdefaultencoding('utf8')
import spider_log  ########
import html_table_reader
import PhantomJS_driver
PhantomJS_driver = PhantomJS_driver.PhantomJS_driver()
html_table_reader = html_table_reader.html_table_reader()
log_obj = spider_log.spider_log() #########

with open('states_belonging.json') as f:
    states_belonging = json.load(f, encoding='utf8')

key_dict = {
    u'宗地坐落':'parcel_location',
    u'宗地编号':'parcel_no',
    u'宗地面积':'offer_area_m2',
    u'容积率':'plot_ratio',
    u'土地用途':'purpose',
    u'起始价':'starting_price_sum',
    u'地块编号':'parcel_no',
    u'地块位置':'parcel_location',
    u'成交价(万元)':'transaction_price_sum'
}

class Spider(scrapy.Spider):
    name = "511701"

    def start_requests(self):
        urls1 =  ["http://www.zjdlr.gov.cn/col/col1071192/index.html?uid=4228212&pageNum=%s" %i for i in xrange(5) if i > 0]
        urls2 =  ["http://www.zjdlr.gov.cn/col/col1071194/index.html?uid=4228212&pageNum=%s" %i for i in xrange(5) if i > 0]
        for url in urls1 + urls2:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        try:
            """结构不同，使用正则表达式直接读取"""
            root_site = "http://www.zjdlr.gov.cn"
            bs_obj = bs4.BeautifulSoup(PhantomJS_driver.get_html(response.url), 'html.parser')
            #log_obj.update_error(bs_obj.prettify(encoding='utf8'))
            #rows = re.findall(r"(?<=<record><!\[CDATA\[).*?(?=</record>)", response.text, re.S)
            e_tables = bs_obj.find('div', class_='default_pgContainer').find_all('table')[1:]

            for e_table in e_tables:
                item = announcements_monitor.items.AnnouncementsMonitorItem()
                item['monitor_city'] = '浙江'

                item['monitor_id'] = self.name
                item['monitor_title'] = e_table.a.get_text(strip=True) # 出让公告标题
                item['monitor_date'] = e_table.find('td', class_='bt_time').get_text(strip=True) # 发布日期
                item['monitor_url'] = root_site + e_table.a.get('href') # 链接

                if re.search(r'.*公告.*', item['monitor_title'].encode('utf8')):
                    item['parcel_status'] = 'onsell'
                    yield scrapy.Request(url=item['monitor_url'], meta={'item': item}, callback=self.parse1,dont_filter=False)
                elif re.search(r'.*公示.*', item['monitor_title'].encode('utf8')):
                    item['parcel_status'] = 'sold'
                    yield scrapy.Request(url=item['monitor_url'], meta={'item': item}, callback=self.parse2,dont_filter=False)
                else:
                    yield item
        except:
            log_obj.update_error("%s中无法解析\n原因：%s" % (self.name, traceback.format_exc()))

    def parse1(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']

        try:
            e_tables = bs_obj.find_all('table', style='border-collapse:collapse; border-color:#333333;font-size:12px;')
            l = []
            for e_table in e_tables:
                df = html_table_reader.table_tr_td(e_table)
                l.append(df)
            item['content_detail'] = l
            item['monitor_extra'] = pd.DataFrame({u"city0": self.get_city0(item['monitor_title'])}, index=[0, ])
            yield item
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" % (self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

    def parse2(self, response):
        """关键词：.*公示.*"""
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        try:
            e_table = bs_obj.find('table', style='border-collapse:collapse; border-color:#333333; font-size:12px;')
            df = html_table_reader.table_tr_td(e_table)
            item['content_detail'] = df
            item['monitor_extra'] = pd.DataFrame({u"city0":self.get_city0(item['monitor_title'])}, index=[0,])
            yield item
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" % (self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

    def get_city0(self, title):
        m = re.search(ur'.{2}市|.{2,6}县', title)

        if m:
            city0 = m.group()
            if city0 in states_belonging:
                city0 = states_belonging[city0]
            elif city0 in states_belonging.values():
                pass
            else:
                city0 = u'未知'
        else:
            city0 = u'未知'
        return city0

if __name__ == '__main__':
    pass
