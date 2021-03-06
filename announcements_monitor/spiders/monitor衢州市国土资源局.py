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
import spider_func
spider_func = spider_func.spider_func()
log_obj = spider_log.spider_log() #########

with open(os.getcwd() + r'\announcements_monitor\spiders\needed_data.txt', 'r') as f:
    s = f.read()
    needed_data = s.split(',')

# key为标题的长度
title_type1 = {11:['parcel_no', 'parcel_location', 'offer_area_m2', 'purpose', 'plot_ratio',
                   '建筑密度', '绿地率', '投资强度（万元/公顷）', '使用年限（年）',
                    'starting_price_sum', '竞买保证金（万元）'],
               13:['parcel_no', 'parcel_location', 'offer_area_m2', 'purpose', 'plot_ratio',
                   '建筑密度', 'building_area', '建筑高度', '绿地率', '投资强度（万元/公顷）',
                  '使用年限（年）','starting_price_sum', '竞买保证金（万元）']}
title_type2 = {14:['parcel_no', 'parcel_location', 'offer_area_m2', 'purpose', '土地级别',
                    'plot_ratio', 'building_area', '供地方式', 'competitive_person', 'transaction_price_sum',
                    '成交时间', '约定开工时间', '约定竣工时间'],
               13:['parcel_name', 'parcel_location', 'offer_area_m2', 'purpose', '土地级别',
                    'plot_ratio', '规划密度', '出让年限', '供地方式', 'competitive_person', 'transaction_price_sum',
                    '成交时间', '备注']}

class Spider(scrapy.Spider):
    name = "511710"

    def start_requests(self):
        self.url1 = "http://www.quzgt.gov.cn/News/Newslist1.aspx?catalogid=180&&parentid=130"
        self.url2 = "http://www.quzgt.gov.cn/News/Newslist1.aspx?catalogid=179&&parentid=130"

        yield scrapy.Request(url=self.url1, callback=self.parse)
        yield scrapy.Request(url=self.url2, callback=self.parse)

    def parse(self, response):
        try:
            bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
            """在使用chrome等浏览器自带的提取extract xpath路径的时候,
                导致明明在浏览器中提取正确, 却在程序中返回错误的结果"""
            e_table = bs_obj.find('div', class_='list_text')
            e_trs = e_table.table.find_all('tr', recursive=False)[:-1]
            for e_tr in e_trs[:-1]:
                item = announcements_monitor.items.AnnouncementsMonitorItem()
                item['monitor_city'] = '衢州'

                e_tds = e_tr.find_all('td')
                item['monitor_id'] = self.name
                item['monitor_title'] = e_tds[1].get_text(strip=True) # 标题
                item['monitor_date'] = e_tds[2].get_text(strip=True) # 成交日期 site.xpath('td[3]/text()').extract_first()
                item['monitor_url'] = "http://www.quzgt.gov.cn/News/" + e_tds[1].a.get('href')

                if response.url == self.url1:
                    item['parcel_status'] = 'onsell'
                    yield scrapy.Request(url=item['monitor_url'], meta={'item': item}, callback=self.parse1, dont_filter=True)
                elif response.url == self.url2:
                    item['parcel_status'] = 'sold'
                    yield scrapy.Request(url=item['monitor_url'], meta={'item': item}, callback=self.parse2, dont_filter=True)
                else:
                    yield item
        except:
            log_obj.update_error("%s中无法解析\n原因：%s" %(self.name, traceback.format_exc()))

    def parse1(self, response):
        """onsell"""
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        try:
            item['content_detail'], item['monitor_extra'] = spider_func.df_output(bs_obj, self.name, item['parcel_status'])
            yield item
        except:
            log_obj.error(item['monitor_url'], "%s（ %s ）中无法解析\n%s" % (self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

    def parse2(self, response):
        """sold"""
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']

        try:
            item['content_detail'], item['monitor_extra'] = spider_func.df_output(bs_obj, self.name, item['parcel_status'])
            yield item
        except:
            log_obj.error(item['monitor_url'], "%s（ %s ）中无法解析\n%s" % (self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

if __name__ == '__main__':
    pass
"""
bs = bs4.BeautifulSoup(s,'html.parser')
bs.find_all('tr')
e_trs = bs.find_all('tr')
[e_trs[0].get_text(), e_trs[1].get_text()]
"""