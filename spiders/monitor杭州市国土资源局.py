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

class Spider(scrapy.Spider):
    name = "511703"

    def start_requests(self):
        self.url1 = ["http://www.hzgtj.gov.cn/fore/portal/infos/toList?parentIdStr=1-32-6686-&id=6686",]
        self.url2 = ["http://www.hzgtj.gov.cn/fore/portal/infos/toList?parentIdStr=1-32-6285-&id=6285",]

        for url in self.url1 + self.url2:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        sel = scrapy.Selector(response)
        root_path = '/html/body/div[4]/div[2]/form/div[3]/table/tbody/tr/td/table[1]/tbody/tr'
        sites = sel.xpath(root_path)  # [@id="list"] [@class="padding10"][position()>1]
        """在使用chrome等浏览器自带的提取extract xpath路径的时候,
           通常现在的浏览器都会对html文本进行一定的规范化,
           导致明明在浏览器中提取正确, 却在程序中返回错误的结果"""
        if not sites:
            sites = sel.xpath(root_path.replace("/tbody",""))

        for site in sites:
            item = announcements_monitor.items.AnnouncementsMonitorItem()
            item['monitor_city'] = '杭州'
            try:
                item['monitor_id'] = self.name
                item['monitor_title'] = re.sub(r"[ \s]", "", site.xpath('td[2]/a/@title').extract_first()) # 标题
                item['monitor_date'] = re.sub(r"[ \s]", "", site.xpath('td[3]/text()').extract_first()) # 成交日期 site.xpath('td[3]/text()').extract_first()
                item['monitor_url'] = "http://www.hzgtj.gov.cn" + site.xpath('td[2]/a/@href').extract_first() # 链接

                if response.url in self.url1:
                    item['parcel_status'] = 'onsell'
                    yield scrapy.Request(item['monitor_url'],meta={'item':item},callback=self.parse1, dont_filter=True)
                elif response.url in self.url2:
                    item['parcel_status'] = 'sold'
                    yield scrapy.Request(item['monitor_url'],meta={'item':item},callback=self.parse1, dont_filter=True)
                else:
                    yield item
            except:
                log_obj.update_error("%s中无法解析\n原因：%s" %(self.name, traceback.format_exc()))


    def parse1(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        try:
            item['content_detail'],item['monitor_extra'] = spider_func.df_output(bs_obj,self.name,item['parcel_status'])
            yield item
        except:
            log_obj.error(item['monitor_url'], "%s（ %s ）中无法解析\n%s" % (self.name, response.url, traceback.format_exc()))
            yield response.meta['item']


if __name__ == '__main__':
    pass