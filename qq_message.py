# -*- coding:utf-8 -*-  
"""
--------------------------------
    @Author: Dyson
    @Contact: Weaver1990@163.com
    @file: qq_message.py
    @time: 2017/8/22 15:37
--------------------------------
"""
import sys
import os
import math

import win32gui

import time
import win32con
import win32clipboard as clipboard

sys.path.append(sys.prefix + "\\Lib\\MyWheels")
reload(sys)
sys.setdefaultencoding('utf8')

class qq_message(object):
    def __init__(self):
        pass

    # 原理是先将需要发送的文本放到剪贴板中，然后将剪贴板内容发送到qq窗口
    # 之后模拟按键发送enter键发送消息

    def getText(self):
        """获取剪贴板文本"""
        clipboard.OpenClipboard()
        d = clipboard.GetClipboardData(win32con.CF_UNICODETEXT)
        clipboard.CloseClipboard()
        return d

    def setText(self, aString):
        """设置剪贴板文本"""
        clipboard.OpenClipboard()
        clipboard.EmptyClipboard()
        clipboard.SetClipboardData(win32con.CF_UNICODETEXT, aString)
        clipboard.CloseClipboard()

    def send_qq(self, to_who, msg):
        """发送qq消息
        to_who：qq消息接收人
        msg：需要发送的消息
        """
        # 将消息写到剪贴板
        self.setText(msg)
        # 获取qq窗口句柄
        qq = win32gui.FindWindow(None, to_who)
        # 投递剪贴板消息到QQ窗体
        win32gui.SendMessage(qq, 258, 22, 2080193)
        win32gui.SendMessage(qq, 770, 0, 0)
        # 模拟按下回车键
        win32gui.SendMessage(qq, win32con.WM_KEYDOWN, win32con.VK_RETURN, 0)
        win32gui.SendMessage(qq, win32con.WM_KEYUP, win32con.VK_RETURN, 0)

    def short_msg(self, to_who, msg, line_count=35):
        l = msg.split(u'\n')
        for i in xrange(int(math.ceil(float(len(l))/line_count))):
            self.send_qq(to_who,u'\n'.join(l[i*line_count : (i+1)*line_count]))
            time.sleep(1)

if __name__ == '__main__':
    # 测试
    qq_message = qq_message()
    to_who = u'【工作号】刘'
    msg = u'这是测试消息\n' * 100
    qq_message.short_msg(to_who, msg)
