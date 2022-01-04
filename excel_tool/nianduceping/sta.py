# -*- coding = utf-8 -*-
# @Time : 2021/12/10 9:39
# @Author: shrgginG
# @File : sta.py
# @Software: PyCharm

import os
import pandas as pd
import collections

result = collections.defaultdict(dict)
names = ['卢俊羲', '田丽萍', '孙超世', '王瑞', '贺宇辰', '曹梦琪', '邵佳伟', '陈楠楠', '安建武', '王大莹', '王振锋', '周稳', '高凯', '韩豪杰', '邹杰', '朱晋恺',
         '江初晴', '陈诗扬', '章轶群', '杨功宇', '吴晓庆', '胡艳娜', '鲍怡蕾', '陈兴', '赵凡', '黄海明', '毛润', '乐鑫', '李沛霖', '魏亮', '王泽宇']
for name in names:
    result[name]['优秀'] = 0
    result[name]['合格'] = 0
    result[name]['基本合格'] = 0
    result[name]['不合格'] = 0

path = "F:/MyProjects/Tools/excel_tool/nianduceping/files"  # 文件夹目录
files = os.listdir(path)

for i in files:
    print(i)
    excel_file = pd.read_excel('./files/' + i)
    for index, row in excel_file.iterrows():
        if index > 6:
            for flag in range(7, 11):
                if row[flag] == '⚪':
                    if (flag == 7):
                        result[row[1]]['优秀'] = result[row[1]]['优秀'] + 1
                    elif (flag == 8):
                        result[row[1]]['合格'] = result[row[1]]['合格'] + 1
                    elif (flag == 9):
                        result[row[1]]['基本合格'] = result[row[1]]['基本合格'] + 1
                    else:
                        result[row[1]]['不合格'] = result[row[1]]['不合格'] + 1

tmp = {}
for name, dict in result.items():
    tmp[name] = dict['优秀']
print(sorted(tmp.items(),key=lambda item:item[1]))