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

import numpy as np
import pandas as pd
import scrapy
import announcements_monitor.items
import re
import traceback
import datetime
import bs4
import json

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
sys.path.append(os.getcwd()) #########
reload(sys)
sys.setdefaultencoding('utf8')
import spider_log  ########
import html_table_reader
html_table_reader = html_table_reader.html_table_reader()
import spider_func
spider_func = spider_func.spider_func()
log_obj = spider_log.spider_log() #########

class Spider(scrapy.Spider):
    name = "511708"

    def start_requests(self):
        # 嘉兴相应网址的index的系数，index_1代表第二页
        self.urls1 = ["http://www.jxgtzy.gov.cn/tdsc/tdgycr/tdzpgxxgg/index.html", ] + ["http://www.jxgtzy.gov.cn/tdsc/tdgycr/tdzpgxxgg/index_%s.html" %i for i in xrange(2) if i > 0]
        self.urls2 = ["http://www.jxgtzy.gov.cn/tdsc/tdgycr/tdcrjggs/index.html", ] + ["http://www.jxgtzy.gov.cn/tdsc/tdgycr/tdcrjggs/index_%s.html" % i for i in xrange(2) if i > 0]

        for url in self.urls1 + self.urls2:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        try:
            bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
            """在使用chrome等浏览器自带的提取extract xpath路径的时候,
                导致明明在浏览器中提取正确, 却在程序中返回错误的结果"""
            e_row = bs_obj.find_all('table', style='line-height:20pt;border-bottom:1px dashed #b0b6de')
            for e_tr in e_row:
                item = announcements_monitor.items.AnnouncementsMonitorItem()
                item['monitor_city'] = '嘉兴'
                e_tds = e_tr.find_all('td')

                item['monitor_id'] = self.name
                item['monitor_title'] = e_tds[0].get_text(strip=True) # 标题
                item['monitor_date'] = e_tds[1].get_text(strip=True) # 成交日期 site.xpath('td[3]/text()').extract_first()
                if response.url in self.urls1:
                    item['parcel_status'] = 'onsell'
                    item['monitor_url'] = "http://www.jxgtzy.gov.cn/tdsc/tdgycr/tdzpgxxgg" + re.sub(ur'\./', '/',e_tds[0].a.get('href'))
                    yield scrapy.Request(item['monitor_url'],meta={'item':item},callback=self.parse1, dont_filter=True)
                elif response.url in self.urls2:
                    item['parcel_status'] = 'sold'
                    item['monitor_url'] = "http://www.jxgtzy.gov.cn/tdsc/tdgycr/tdcrjggs" + re.sub(ur'\./', '/',e_tds[0].a.get('href'))  # 链接
                    yield scrapy.Request(item['monitor_url'],meta={'item':item},callback=self.parse1, dont_filter=True)
                else:
                    yield item
        except:
            log_obj.update_error("%s中无法解析\n原因：%s" %(self.name, traceback.format_exc()))

    def parse1(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        try:
            e_table = bs_obj.find('table', class_='MsoNormalTable')
            if not e_table:
                e_table = bs_obj.find('div', align='center').table
            df = html_table_reader.table_tr_td(e_table)
            item['content_detail'] = df
            if item['parcel_status'] == 'onsell':
                try:
                    item['monitor_extra'] = spider_func.extra_parse(bs_obj,{"tag": "div","attrs": {"class": "TRS_PreAppend"},"row_tag": "p"})
                except:
                    item['monitor_extra'] = spider_func.extra_parse(bs_obj, {"tag": "td", "attrs": {"class": "text"}, "row_tag": "p"})
            yield item
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" %(self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

if __name__ == '__main__':
    pass