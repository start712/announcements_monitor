# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: controller.py
    @time: 2017/4/18 13:54
--------------------------------
"""
import sys
import os
import datetime

import re

import pymail
import time
import csv
import qq_message
qq_message = qq_message.qq_message()

import TuPaiWang_update
TuPaiWang_update = TuPaiWang_update.TuPaiWang_update()

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')

csv_file = os.getcwd() + r'\log\NEW(%s -%s).csv' %(datetime.datetime.date(datetime.datetime.today()),
                         round(datetime.datetime.today().hour + datetime.datetime.today().minute/60))
print 'csv_file',csv_file
 #新公告将储存在这个文件中

class controller(object):
    def __init__(self):
        self.pymail = pymail.pymail()

    def start_spider(self, spider_id):
        """通过cmd命令启动爬虫"""
        if spider_id != "monitor":
            spider_id = " " + spider_id
        os.system("scrapy %s" %spider_id)

    def min_run(self):
        # 启动爬虫
        qq_message.send_qq(u'【工作号】刘',u'开始爬虫作业')
        time.sleep(1)
        self.start_spider('monitor')
        print u'爬虫运行完毕'

        self.report()

    def report(self):

        name_list = [u'【工作号】刘', u'Mr.Yao', u'qxq']
    
        # 有新内容的话，发送邮件
        report_file = [csv_file, ]
        if os.path.exists(csv_file):
            s = ""
            with open(report_file[0], 'rb') as f:
                rows = csv.reader(f)
                for row in rows:
                    if row:
                        if re.search(r'工', row[0]):
                            continue
                        s = s + ",".join(row) + '\n'

            for id0 in name_list:
                qq_message.short_msg(id0,s.decode('utf8'))

            print u'找到新地块,QQ消息已发送'

        else:
            for id0 in name_list:
                qq_message.short_msg(id0,u"无新公告！！")

        # 土拍网土地监控
        TuPaiWang_update.main(name_list)
        
        # 发送log
        date0 = datetime.datetime.date(datetime.datetime.today() + datetime.timedelta(days=-1))
        log_file = [
            r'%s/log/pipelines_error(%s).log' % (os.getcwd(), date0),
            r'%s/log/spider_DEBUG(%s).log' % (os.getcwd(), date0)
        ]
        if os.path.exists(log_file[0]) and os.path.exists(log_file[1]):
            title = "日常报告%s" % date0
            if os.path.getsize(log_file[1]) == 0:
                title = "新的BUG报告%s" % date0

            self.pymail.try_send_mail(log_file, title, txt="", to_mail='3118734521@qq.com')
            self.pymail.try_send_mail(log_file, title, txt="", to_mail='415281457@qq.com')
            for f in log_file:  # 发送过的文件，改掉文件名，避免多次发送发给
                os.rename(f, f + "_")
        elif os.path.exists(log_file[0] + "_") and os.path.exists(log_file[1] + "_"):
            print u'不需要发送log邮件'
        else:
            self.pymail.try_send_mail(None, "爬虫报告%s缺少文件" % date0, to_mail='3118734521@qq.com')
            self.pymail.try_send_mail(None, "爬虫报告%s缺少文件" % date0, to_mail='415281457@qq.com')
            
            
 


if __name__ == '__main__':
    controller = controller()
    
    controller.min_run()

    
