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
    name = "500009"

    def start_requests(self):
        self.urls = ["http://tdjy.zjdlr.gov.cn/GTJY_ZJ/go_home",]
        for url in self.urls:
            yield scrapy.Request(url=url, callback=self.parse)

    def parse(self, response):

        # 统计总共多少页
        driver = PhantomJS_driver.initialization()
        driver.get('about:blank')
        driver.get(response.url)
        driver.switch_to.frame('contentmain')
        driver.find_element_by_class_name('a1').click()

        e_div = driver.find_element_by_class_name('content_fy')
        e_div.find_element_by_link_text('末页').click()
        total_page = int(e_div.find_element_by_tag_name('strong').text)

        print "一共有%s页" %total_page
        page_dict = {(i + 1): None for i in range(total_page)}
        print page_dict
        current_row = [1,1]
        driver.quit()

        detail_page_row = 1
        detail_page_row_count = 1
        while True:
            page, row = current_row
            try:
                driver = PhantomJS_driver.initialization()
                driver.get('about:blank')
                driver.get(response.url)
                driver.switch_to.frame('contentmain')
                driver.find_element_by_class_name('a1').click()

                # 翻页
                read_page = 1
                for i in range(page-1):
                    e_div = driver.find_element_by_class_name('content_fy')
                    e_div.find_element_by_link_text('下一页').click()
                    read_page = e_div.find_element_by_tag_name('strong').text
                print '目前页面，第%s页' % read_page


                # 开始分析行数据
                driver.switch_to.frame('noticelist_main')
                e_trs = driver.find_elements_by_tag_name('tr')[1:]

                # 统计每一页中有几行
                if page_dict[page] is None:
                    page_dict[page] = len(e_trs)
                    print "第%s页中有%s行" %(page, page_dict[page])

                print "分析第%s行中。。。" %row
                e_row = e_trs[row-1]
                title0 = e_row.find_elements_by_tag_name('td')[1].text

                detail_button = e_row.find_element_by_tag_name('a')
                detail_button.click()
                driver.switch_to_window(driver.window_handles[-1])

                # 详情页还有几行数据需要点击
                #driver.get_screenshot_as_file('C:\Users\Administrator\Desktop\data.png')

                driver.switch_to.frame('contentmain')
                driver.switch_to.frame('resslist_main')

                e_table = driver.find_element_by_tag_name('table')
                e_trs = e_table.find_elements_by_tag_name('tr')[1:]
                detail_page_row_count = len(e_trs)

                e_tr = e_trs[detail_page_row-1]

                title = e_tr.find_elements_by_tag_name('td')[1].text

                detail_button = e_tr.find_element_by_tag_name('a')
                detail_button.click()
                driver.switch_to_window(driver.window_handles[-1])

                driver.switch_to.frame('contentmain')
                e_div = driver.find_element_by_class_name('tab_list')
                e_a = e_div.find_elements_by_tag_name('a')[5]
                e_a.click()
                driver.switch_to_window(driver.window_handles[-1])

                #driver.get_screenshot_as_file('C:\Users\Administrator\Desktop\data.png')
                #with open('C:\Users\Administrator\Desktop\data.html', 'w') as f:
                #    f.write(driver.page_source)

                bs_obj = bs4.BeautifulSoup(driver.page_source, 'html.parser')
                e_div = bs_obj.find('div', class_='xs_list_table')

                title1_list = [e.get_text(strip=True) for e in e_div.find_all('tr')[1:]]

                html_text = e_div.prettify(encoding='utf8')

                comment_list = re.findall(r'\<\!--.+?--\>', html_text, re.S)
                #print comment_list
                for i in range(len(comment_list)):
                    print "正在分析%s， 一共%s个文件" %(title1_list[i],len(comment_list))
                    s = comment_list[i]
                    m = re.search(r"(?<=\<a href\=\" ).+?(?=\")", s)
                    file_url = 'http://tdjy.zjdlr.gov.cn/GTJY_ZJ/' + m.group()
                    #file_name = re.search(r'(?<=fileName\=).+', file_url).group()
                    PhantomJS_driver.get_file(file_url, 'C:\\Users\\Administrator\\Desktop\\files\\(%s)%s' %(title,title1_list[i]))

                #yield scrapy.Request(item['monitor_url'], meta={'item': item}, callback=self.parse1, dont_filter=True)
            except:
                log_obj.update_error("%s中无法解析\n原因：%s|%s|%s\n%s" %(self.name,page,row,detail_page_row, traceback.format_exc()))

            if detail_page_row < detail_page_row_count:
                detail_page_row = detail_page_row + 1
            else:
                detail_page_row = 1
                if row == page_dict[page]:
                    row = 1
                    page = page + 1
                else:
                    row = row + 1

                if page > total_page:
                    break

            current_row = page, row

            driver.quit()

if __name__ == '__main__':
    pass
