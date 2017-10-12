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

log_path = r'%s/log/spider_DEBUG(%s).log' %(os.getcwd(),datetime.datetime.date(datetime.datetime.today()))

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
sys.path.append(os.getcwd()) #########
reload(sys)
sys.setdefaultencoding('utf8')
import spider_log  ########
import spider_func
import PhantomJS_driver
import time
PhantomJS_driver = PhantomJS_driver.PhantomJS_driver()
spider_func = spider_func.spider_func()
log_obj = spider_log.spider_log() #########

with open(os.getcwd() + r'\announcements_monitor\spiders\needed_data.txt', 'r') as f:
    s = f.read()
    needed_data = s.split(',')
needed_data = [s.encode('utf8') for s in needed_data]

monitor_page = 1  # 监控目录页数

class Spider(scrapy.Spider):
    name = "500003"

    def start_requests(self):
        self.urls = ["http://www.fuyang.gov.cn/fy/ghj/jghs/index_%s.jhtml" %i for i in xrange(monitor_page)]
        for url in self.urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        try:
            bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
            e_ul = bs_obj.find('div', class_='neirong_detail')
            e_lis = e_ul.find_all('li')
            for e_li in e_lis:
                item = announcements_monitor.items.AnnouncementsMonitorItem()
                item['monitor_city'] = '富阳'
                item['parcel_status'] = 'city_planning'

                item['monitor_id'] = self.name
                item['monitor_title'] = u'建设项目规划竣工核实' + e_li.a.get_text(strip=True) # 标题
                item['monitor_date'] = e_li.find('span', class_='time').get_text(strip=True) # 成交日期
                item['monitor_url'] = e_li.a.get('href')

                yield scrapy.Request(item['monitor_url'], meta={'item': item}, callback=self.parse1, dont_filter=True)
        except:
            log_obj.update_error("%s中无法解析\n原因：%s" %(self.name, traceback.format_exc()))

    def parse1(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        try:
            item['content_detail'],item['monitor_extra'] = spider_func.df_output(bs_obj,self.name,item['parcel_status'])
            yield item
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" % (self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

if __name__ == '__main__':
    pass