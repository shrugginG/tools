# -*- coding = utf-8 -*-
# @Time : 2021/11/26 15:30
# @Author: shrgginG
# @File : read_excel.py
# @Software: PyCharm

import pandas as pd


filename='./files/网安（无锡）20级新生分班名单.xlsx'

classmate_DF=pd.read_excel(filename,sheet_name='4班')

for index,row in classmate_DF.iterrows():
    print(row['姓名']+":")
    print("\n")
    print("\n")
    print("\n")
