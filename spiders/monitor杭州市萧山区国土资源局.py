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
import pandas as pd
import scrapy
import announcements_monitor.items
import re
import datetime
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
    name = "511700"

    def start_requests(self):
        self.urls1 = ["http://115.236.5.251/Bulletin/BulletinList.aspx?ProType=13&AfficheType=617&Class=227&ViewID=112",]
        self.urls2 = ["http://115.236.5.251/Bulletin/BulletinList.aspx?ProType=13&AfficheType=619&Class=227&ViewID=241",]
        for url in self.urls1 + self.urls2:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        sel = scrapy.Selector(response)
        root_path = '//tbody/tr[@class="Row"]'
        sites = sel.xpath(root_path)  # [@id="list"] [@class="padding10"][position()>1]
        """在使用chrome等浏览器自带的提取extract xpath路径的时候,
           通常现在的浏览器都会对html文本进行一定的规范化,
           导致明明在浏览器中提取正确, 却在程序中返回错误的结果"""
        if not sites:
            sites = sel.xpath(root_path.replace("/tbody",""))

        for site in sites:
            item = announcements_monitor.items.AnnouncementsMonitorItem()
            item['monitor_city'] = '杭州萧山'
            try:
                #print dir(site.xpath('td[@align="left"]/a/text()'))
                item['monitor_id'] = self.name
                id = site.xpath('td[@align="left"]/a[@target="_blank"]/text()').extract_first() # 招标编号
                title = site.xpath('td[contains(@class,"DispLimitColumn")]/div/a[@target="_blank"]/text()').extract_first() # 标题
                item['monitor_title'] = id + title

                item['monitor_date'] = site.xpath('td[contains(@class,"DateColumn")]/text()').extract_first() # 发布日期
                item['monitor_url'] = 'http://115.236.5.251/Bulletin/' + site.xpath('td[@align="left"]/a/@href').extract_first() # 链接

                if response.url in self.urls1:
                    item['parcel_status'] = 'onsell'
                    yield scrapy.Request(item['monitor_url'], meta={'item': item}, callback=self.parse1, dont_filter=False)
                elif response.url in self.urls2:
                    item['parcel_status'] = 'sold'
                    yield scrapy.Request(item['monitor_url'], meta={'item': item}, callback=self.parse1, dont_filter=False)
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
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" % (self.name, response.url, traceback.format_exc()))
            yield response.meta['item']


if __name__ == '__main__':
    pass