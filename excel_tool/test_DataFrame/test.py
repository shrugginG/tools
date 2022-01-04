# -*- coding = utf-8 -*-
# @Time : 2021/12/30 11:44
# @Author: shrgginG
# @File : test.py
# @Software: PyCharm

import pandas as pd

list = [{"name": "kenny", "age": 20}, {"name": "tom", "age": 12}]
DF = pd.read_csv('./datas/csola_cc.csv')
print(DF['客户端IP'])
