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
log_obj = spider_log.spider_log() #########

with open(os.getcwd() + r'\announcements_monitor\spiders\needed_data.txt', 'r') as f:
    s = f.read()
    needed_data = s.split(',')
needed_data = [s.encode('utf8') for s in needed_data]

title_match = {
    u'地块编号': 'parcel_no',
    u'地块位置': 'parcel_location',
    u'土地面积': 'offer_area_m2',
    u'面积': 'offer_area_m2',
    u'土地用途': 'purpose',
    u'最大容积率': 'plot_ratio',
    u'土地竞得人': 'competitive_person',
    u'成交价': 'transaction_price_sum',
    u'受让单位': 'competitive_person',
    u'宗地编号': 'parcel_no',
    u'宗地面积': 'offer_area_m2',
    u'宗地坐落': 'parcel_location',
    u'容积率': 'plot_ratio',
    u'起始价': 'starting_price_sum',
    u'编号': 'parcel_no',
    '编号': 'parcel_no',
}
title_type = {
    14:['编号', 'parcel_location', '土地总面积', 'offer_area_m2', '退让面积', 'purpose', 'starting_price_sum',
        'plot_ratio', '建筑密度', '绿地率', '出让年限', '竞买保证金', '开工保证金', '竣工保证金'],
    16:['编号', 'parcel_location', '土地总面积', 'offer_area_m2', '退让面积', 'purpose', 'starting_price_sum',
        'plot_ratio', '建筑密度', '绿地率', '出让年限', '投资总额', '竞买保证金', '开工保证金', '竣工保证金', '履约保证金']
}
title_height = {14:2, 16:2}

class Spider(scrapy.Spider):
    name = "511713"

    def start_requests(self):
        # 舟山相应网址的index的系数，index_2代表第二页
        self.urls1 = ["http://www.zsblr.gov.cn/mlx/tdsc/tdzpgxxgg/index.html",] + ["http://www.zsblr.gov.cn/mlx/tdsc/tdzpgxxgg/index_%s.html" %i for i in xrange(3) if i > 1]
        self.urls2 = ["http://www.zsblr.gov.cn/mlx/tdsc/tdcrjg/index.html",] + ["http://www.zsblr.gov.cn/mlx/tdsc/tdcrjg/index_%s.html" %i for i in xrange(3) if i > 1]

        for url in self.urls1 + self.urls2:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        """在使用chrome等浏览器自带的提取extract xpath路径的时候,
            导致明明在浏览器中提取正确, 却在程序中返回错误的结果"""
        #e_table = bs_obj.find('div', class_='txtlist')
        e_table = bs_obj.find('div', id='mlx_list')
        e_row = e_table.find_all('li')
        for e_tr in e_row:
            item = announcements_monitor.items.AnnouncementsMonitorItem()
            item['monitor_city'] = '舟山'
            try:
                item['monitor_id'] = self.name
                item['monitor_title'] = e_tr.a.get_text(strip=True) # 标题
                item['monitor_date'] = e_tr.span.get_text(strip=True) # 成交日期
                item['monitor_url'] = "http://www.zsblr.gov.cn" + e_tr.a.get('href')

                if response.url in self.urls1 and not re.search(ur'海域', item['monitor_title']):
                    item['parcel_status'] = 'onsell'
                    yield scrapy.Request(item['monitor_url'], meta={'item': item}, callback=self.parse1, dont_filter=True)
                elif response.url in self.urls2:
                    item['parcel_status'] = 'sold'
                    yield scrapy.Request(item['monitor_url'], meta={'item': item}, callback=self.parse1, dont_filter=True)
                else:
                    yield item
            except:
                log_obj.update_error("%s中无法解析%s\n原因：%s" %(self.name, e_tr, traceback.format_exc()))

    def parse1(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        try:
            e_table = bs_obj.table
            while e_table.table:
                e_table = e_table.table

            df = html_table_reader.table_tr_td(e_table)
            item['content_detail'] = df
            yield item
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" % (self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

if __name__ == '__main__':
    pass

"""
bs = bs4.BeautifulSoup(s,'html.parser')
e_table = bs.table
while e_table.table:
    e_table = e_table.table
"""