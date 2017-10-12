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
import requests
import numpy as np

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
sys.path.append(os.getcwd()) #########
reload(sys)
sys.setdefaultencoding('utf8')
import spider_log  ########
import spider_func
spider_func = spider_func.spider_func()
log_obj = spider_log.spider_log() #########

re_table = {u'地块名称':'parcel_no',
            u'土地用途':'purpose',
            u'土地面积':'offer_area_m2',
            u'容积率':'plot_ratio',
            u'土地坐落':'parcel_location',
            u'土地座落':'parcel_location',
            u'建筑面积':'building_area',
            u'起拍价':'starting_price_sum',
            u'起始价':'starting_price_sum',
            }

class Spider(scrapy.Spider):
    name = "511706"

    def start_requests(self):
        self.url1 = "http://www.sxztb.gov.cn/sxweb/tdjy/005006/005006004/005006004001/MoreInfo.aspx?CategoryNum=005006004001"
        self.url2 = "http://www.sxztb.gov.cn/sxweb/tdjy/005006/005006005/005006005001/MoreInfo.aspx?CategoryNum=005006005001"
        yield scrapy.Request(url=self.url1, callback=self.parse)
        yield scrapy.Request(url=self.url2, callback=self.parse)

    def parse(self, response):
        try:
            bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
            e_table = bs_obj.find('table', id='MoreInfoList1_DataGrid1')
            """在使用chrome等浏览器自带的提取extract xpath路径的时候,
               通常现在的浏览器都会对html文本进行一定的规范化,
               导致明明在浏览器中提取正确, 却在程序中返回错误的结果"""
            for e_tr in e_table.find_all('tr'):
                item = announcements_monitor.items.AnnouncementsMonitorItem()
                item['monitor_city'] = '绍兴'
                e_tds = e_tr.find_all('td')

                item['monitor_id'] = self.name
                item['monitor_title'] = e_tds[1].get_text(strip=True) # 标题
                item['monitor_date'] = e_tds[2].get_text(strip=True) # 发布日期
                item['monitor_url'] = 'http://www.sxztb.gov.cn' + e_tds[1].a.get('href') # 链接

                if re.search(ur'绍兴市国土资源局国有建设用地使用权.*|绍兴市国土资源局上虞区分局国有建设用地使用权出让公告.*', item['monitor_title']):
                    item['parcel_status'] = 'onsell'
                    yield scrapy.Request(url=item['monitor_url'], meta={'item': item}, callback=self.parse1, dont_filter=True)
                elif response.url == self.url2:
                    item['parcel_status'] = 'sold'
                    yield scrapy.Request(url=item['monitor_url'], meta={'item': item}, callback=self.parse1, dont_filter=True)
                else:
                    yield item
        except:
            log_obj.update_error("%s中无法解析%s\n原因：%s" %(self.name, e_tr, traceback.format_exc()))

    def parse1(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        try:
            item['content_detail'], item['monitor_extra'] = spider_func.df_output(bs_obj, self.name, item['parcel_status'])
            yield item
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" % (self.name, response.url, traceback.format_exc()))
            yield response.meta['item']


if __name__ == '__main__':
    pass

"""
	
ASP.NET_SessionId=nyfwwyjh2ul0et550monou45	
ASP.NET_SessionId=nyfwwyjh2ul0et550monou45
"""