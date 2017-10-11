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


with open(os.getcwd() + r'\announcements_monitor\spiders\needed_data.txt', 'r') as f:
    s = f.read()
    needed_data = s.split(',')
needed_data = [s.encode('utf8') for s in needed_data]

re_text = {
    ur'土地坐落：.+?。': 'parcel_location',
    ur'土地用途：.+?。': 'purpose',
    ur'出让年限：.+?。': '出让年限'
}

class Spider(scrapy.Spider):
    name = "511712"

    def start_requests(self):
        # 丽水相应网址的index的系数，index_1代表第二页
        self.urls1 = ["http://www.lssgtzyj.gov.cn/ArticleList/Index/284?pageIndex=%s&title=" %i for i in xrange(3) if i > 0]
        #self.urls2 = ["http://www.zjtzgtj.gov.cn/scxx/tdsc/tdcrcj/index_%s.html" %i for i in xrange(3) if i > 0]

        for url in self.urls1:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        """在使用chrome等浏览器自带的提取extract xpath路径的时候,
            导致明明在浏览器中提取正确, 却在程序中返回错误的结果"""
        #e_table = bs_obj.find('div', class_='txtlist')
        e_row = bs_obj.find_all('div', class_='Lcon02R-2-01')
        for e_tr in e_row:
            item = announcements_monitor.items.AnnouncementsMonitorItem()
            item['monitor_city'] = '丽水'
            try:
                item['monitor_id'] = self.name
                item['monitor_title'] = e_tr.a.get_text(strip=True) # 标题
                item['monitor_date'] = e_tr.find('p', class_='p21').get_text(strip=True) # 成交日期
                item['monitor_url'] = "http://www.lssgtzyj.gov.cn" + e_tr.a.get('href')

                yield scrapy.Request(item['monitor_url'], meta={'item': item}, callback=self.parse1, dont_filter=True)
            except:
                log_obj.update_error("%s中无法解析%s\n原因：%s" %(self.name, e_tr, traceback.format_exc()))

    def parse1(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']
        item['parcel_status'] = 'onsell'
        try:
            e_page = bs_obj.find('div', attrs={'id':'infoContent', 'class':'SconC'})
            # 处理网页文字
            e_ps = e_page.find_all('p')
            row_ps = [e_p.get_text(strip=True) for e_p in e_ps]
            d = {re_text[r]:filter(lambda x: re.search(r, x).group() if isinstance(x,unicode) and
                                  re.search(r, x) else None, row_ps) for r in re_text}
            df0 = pd.DataFrame(d)
            item['monitor_extra'] = df0

            # 处理网页中的表格
            e_table = e_page.table
            df = html_table_reader.table_tr_td(e_table)
            item['content_detail'] = df
            yield item
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" %(self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

if __name__ == '__main__':
    pass