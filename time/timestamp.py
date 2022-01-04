# -*- coding = utf-8 -*-
# @Time : 2022/1/4 11:02
# @Author: shrgginG
# @File : timestamp.py
# @Software: PyCharm
import time

# 毫米级时间戳是13位
time_in_ms = 1611208640592
# 转换为东八区时间
timelocal = time.localtime(time_in_ms / 1000.0)
# 将毫米级时间戳格式化位可读格式
formatted_time = time.strftime('%Y-%m-%d %H:%M:%S:{}'.format(time_in_ms % 1000), timelocal)

print(formatted_time)
