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
import scrapy
import announcements_monitor.items
import re
import traceback
import datetime
import bs4
import json
import pandas as pd

log_path = r'%s/log/spider_DEBUG(%s).log' %(os.getcwd(),datetime.datetime.date(datetime.datetime.today()))

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
sys.path.append(os.getcwd())
reload(sys)
sys.setdefaultencoding('utf8')
import spider_log  ########
import html_table_reader
html_table_reader = html_table_reader.html_table_reader()
log_obj = spider_log.spider_log() #########

class Spider(scrapy.Spider):
    name = "511715"

    def start_requests(self):
        self.url1 = "http://www.wzgt.gov.cn/Face.aspx?FaceNodeID=31&FaceItemID=24811"
        self.url2 = 'http://www.wzgt.gov.cn/Face.aspx?FaceNodeID=32'
        yield scrapy.Request(url=self.url1, callback=self.parse1)
        yield scrapy.Request(url=self.url1, callback=self.parse2)

    def parse1(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        e_table = bs_obj.find('table', id='Control_12818')

        e_trs = e_table.find_all('tr')[1:-1]
        for e_tr in e_trs:
            item = announcements_monitor.items.AnnouncementsMonitorItem()
            item['monitor_city'] = '温州'

            try:
                e_tds = e_tr.find_all('td')
                item['monitor_id'] = self.name
                item['monitor_title'] = e_tds[0].get_text(strip=True) # 标题
                item['monitor_date'] = e_tds[-1].get_text(strip=True) # 成交日期
                item['monitor_url'] = e_tds[0].get('href')
                item['monitor_extra'] = json.dumps({
                    u'出让面积': e_tds[1].get_text(strip=True),
                    u'出让方式': e_tds[2].get_text(strip=True),
                    u'用途': e_tds[3].get_text(strip=True),
                    u'起始价': e_tds[4].get_text(strip=True),
                },ensure_ascii=False)

                yield scrapy.Request(item['monitor_url'], meta={'item': item}, callback=self.parse1, dont_filter=True)
            except:
                log_obj.update_error("%s中无法解析%s\n原因：%s" %(self.name, e_tr, traceback.format_exc()))


    def parse2(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        e_table = bs_obj.find('table', id='Control_27133')
        e_trs = e_table.find_all('tr')[-1]
        for e_tr in e_trs:
            item = announcements_monitor.items.AnnouncementsMonitorItem()
            item['monitor_city'] = '温州'

            try:
                item['monitor_id'] = self.name
                item['monitor_title'] = e_tr.a.get_text(strip=True) # 标题
                item['monitor_date'] = e_tr.find_all('div')[-1].get_text(strip=True) # 成交日期
                item['monitor_url'] = e_tr.a.get('href')

                yield scrapy.Request(item['monitor_url'], meta={'item': item}, callback=self.parse1, dont_filter=True)
            except:
                log_obj.update_error("%s中无法解析%s\n原因：%s" %(self.name, e_tr, traceback.format_exc()))

if __name__ == '__main__':
    pass