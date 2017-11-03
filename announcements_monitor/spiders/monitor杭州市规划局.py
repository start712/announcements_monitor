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

log_path = r'%s/log/spider_DEBUG(%s).log' %(os.getcwd(),datetime.datetime.date(datetime.datetime.today()))

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
sys.path.append(os.getcwd()) #########
reload(sys)
sys.setdefaultencoding('utf8')
import spider_log  ########
import spider_func
import PhantomJS_driver
PhantomJS_driver = PhantomJS_driver.PhantomJS_driver()
spider_func = spider_func.spider_func()
log_obj = spider_log.spider_log() #########

with open(os.getcwd() + r'\announcements_monitor\spiders\needed_data.txt', 'r') as f:
    s = f.read()
    needed_data = s.split(',')
needed_data = [s.encode('utf8') for s in needed_data]

class Spider(scrapy.Spider):
    name = "500000"

    def start_requests(self):
        # 临安相应网址的index的系数，index_1代表第二页
        self.urls1 = ["http://www.hzplanning.gov.cn/index.aspx?tabid=6ff05919-8be4-4f8d-a1bc-e923bc5e5a62&type=1", ]
        self.urls2 = ["http://www.hzplanning.gov.cn/index.aspx?tabid=a196201a-27af-4b86-a157-00f097a83a25&type=10", ]
        #self.urls3 = ["http://www.linan.gov.cn/gtzyj/gsgg/cjxx/index.html", ] + ["http://www.linan.gov.cn/gtzyj/gsgg/cjxx/index_%s.html" % i for i in xrange(2) if i > 1]
        for url in self.urls1 + self.urls2:# + self.urls3:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):
        try:
            driver = PhantomJS_driver.initialization()
            driver.get(response.url)
            html_list = []
            monitor_page = 1 # 监控目录页数

            for i in xrange(monitor_page):
                html_list.append(bs4.BeautifulSoup(driver.page_source,'html.parser'))
                driver.find_element_by_link_text(u'[下一页]')

            driver.quit()

            for bs_obj in html_list:
                e_table = bs_obj.find('table', class_='publicityCss')
                e_row = e_table.find_all('tr')[1:]
                for e_tr in e_row:
                    item = announcements_monitor.items.AnnouncementsMonitorItem()
                    item['monitor_city'] = '杭州'
                    item['parcel_status'] = 'city_planning'

                    # 除去标题
                    if not e_tr.a:
                        continue

                    e_tds = e_tr.find_all('td')
                    item['monitor_id'] = self.name
                    item['monitor_title'] = e_tds[0].a.get_text(strip=True) # 标题
                    item['monitor_date'] = e_tds[2].get_text(strip=True) # 成交日期
                    item['monitor_url'] = 'http://www.hzplanning.gov.cn' + e_tds[0].a.get('href')

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

            e_div = bs_obj.find('div', id='gsp')
            img_url = 'http://www.hzplanning.gov.cn/DesktopModules/GHJ.PlanningNotice/' + e_div.a.get('href')

            yield scrapy.Request(img_url, meta={'item': item}, callback=self.parse_img, dont_filter=True)
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" % (self.name, response.url, traceback.format_exc()))
            yield response.meta['item']

    def parse_img(self, response):
        bs_obj = bs4.BeautifulSoup(response.text, 'html.parser')
        item = response.meta['item']

        try:
            print "\n====>Running parse_img \n"
            e_div = bs_obj.find('div', id='ImageDIV')
            img_url = e_div.img.get('src')
            img_url = 'http://www.hzplanning.gov.cn' + re.sub(r'\?r\=.+','',img_url)
            PhantomJS_driver.get_file(img_url, os.getcwd() + 'files\\%s.jpg' %item['monitor_title'])
            yield item
        except:
            log_obj.error(item['monitor_url'], "%s（%s）中无法解析\n%s" % (self.name, response.url, traceback.format_exc()))
            yield response.meta['item']


if __name__ == '__main__':
    pass