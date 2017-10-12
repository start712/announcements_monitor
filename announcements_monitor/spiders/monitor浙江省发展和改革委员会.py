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
    name = "500005"

    def start_requests(self):
        self.urls = ["http://www.zjdpc.gov.cn/col/col8/index.html",]
        for url in self.urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        driver = PhantomJS_driver.initialization()
        driver.get(response.url)
        html_list = []

        for i in xrange(monitor_page):
            html_list.append(bs4.BeautifulSoup(driver.page_source,'html.parser'))
            driver.find_element_by_xpath("//*[@class='default_pgBtn default_pgNext']").click()
            time.sleep(1)

        driver.quit()

        for bs_obj in html_list:
            e_table = bs_obj.find('div', class_='default_pgContainer')
            e_row = e_table.find_all('table')
            for e in e_row:
                item = announcements_monitor.items.AnnouncementsMonitorItem()
                item['monitor_city'] = '浙江'
                item['parcel_status'] = 'city_planning'
                try:
                    e_a = e.a
                    item['monitor_id'] = self.name
                    item['monitor_title'] = e_a.get('title') # 标题
                    item['monitor_date'] = e_a.find_next('td').get_text(strip=True) # 成交日期
                    item['monitor_url'] = 'http://www.zjdpc.gov.cn' + e_a.get('href')

                    yield scrapy.Request(item['monitor_url'], meta={'item': item}, callback=self.parse1, dont_filter=True)
                except:
                    log_obj.update_error("%s中无法解析\n原因：%s" %(self.name, traceback.format_exc()))

    def parse1(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        try:
            df = spider_func.city_planning(self.name, item['monitor_city'], item['monitor_title'], bs_obj)
            if df is not None:
                item['monitor_extra'] = df
                yield item
        except:
            log_obj.error(item['monitor_url'], "%s（ %s ）中无法解析\n%s" % (self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

if __name__ == '__main__':
    pass