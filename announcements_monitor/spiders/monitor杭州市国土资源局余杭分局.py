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
import re

import bs4
import scrapy
import announcements_monitor.items
import datetime

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
sys.path.append(os.getcwd()) #########
reload(sys)
sys.setdefaultencoding('utf8')
import spider_log  ########
import html_table_reader
html_table_reader = html_table_reader.html_table_reader()
log_obj = spider_log.spider_log() #########

class Spider(scrapy.Spider):
    name = "511699"
    allowed_domains = ["www.yhland.gov.cn"]

    def start_requests(self):
        url =  ["http://www.yuhang.gov.cn/xxgk/gggs/td/index.html",] + ['http://www.yuhang.gov.cn/xxgk/gggs/td/index_%s.html' % i for i in xrange(2) if i > 0]
        yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        """在使用chrome等浏览器自带的提取extract xpath路径的时候,
           通常现在的浏览器都会对html文本进行一定的规范化,
           导致明明在浏览器中提取正确, 却在程序中返回错误的结果"""
        try:
            e_table = bs_obj.find('table', class_='ZjYhN018')
            e_trs = e_table.find_all('tr')
            for e_tr in e_trs:
                item = announcements_monitor.items.AnnouncementsMonitorItem()
                e_tds = e_tr.find_all('td')
                e_a = e_tds[1].find_all['a'][-1]
                item['monitor_id'] = self.name
                item['monitor_title'] = e_a.get_text(strip=True) # 宗地名称
                item['monitor_date'] = e_tds[-1].get_text(strip=True) # 成交日期
                item['monitor_url'] = "http://www.yuhang.gov.cn/xxgk/gggs/td" + e_a.get('href') # 链接

                if re.search(ur'权挂牌出让公告', item['monitor_title'].encode('utf8')):
                    item['parcel_status'] = 'onsell'
                    yield scrapy.Request(url=item['monitor_url'], meta={'item': item}, callback=self.parse1, dont_filter=False)
                elif re.search(ur'挂牌结果公示', item['monitor_title'].encode('utf8')):
                    item['parcel_status'] = 'sold'
                    yield scrapy.Request(url=item['monitor_url'], meta={'item': item}, callback=self.parse1, dont_filter=False)
                else:
                    yield item
        except:
            log_obj.update_error("%s中无法解析\n原因：%s" % (self.name, traceback.format_exc()))


    def parse1(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        try:
            e_table = bs_obj.find("table", class_="MsoNormalTable")
            df = html_table_reader.title_standardize(html_table_reader.table_tr_td(e_table), delimiter=r'=>')
            item['content_detail'] = df
            yield item
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" % (self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

if __name__ == '__main__':
    pass