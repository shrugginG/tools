# -*- coding = utf-8 -*-
# @Time : 2021/12/9 9:57
# @Author: shrgginG
# @File : tmp.py
# @Software: PyCharm

#!/usr/bin/env python
# -*- coding:utf-8 -*-
#
import collections

import findspark

findspark.init()

from pyspark.sql import SparkSession
import json
import detection_functions
from elasticsearch import Elasticsearch
import joblib

domain_white_list = ['site', 'qq.com', 'taobao.com', 'jd.com', 'yximgs.com', 'baidu.com', 'sina.com.cn', '360.com',
                     'douyincdn.com', 'vivo.com.cn', 'pstatp.com', 'snssdk.com', 'etoote.com', '360.cn',
                     'oppomobile.com', 'p2cdn.com', 'ixigua.com', 'aliyuncs.com', 'hicloud.com', 'alicdn.com',
                     'ntp.org', 'amap.com', 'microsoft.com', 'google.com', 'sogou.com', 'gifshow.com', 'xycdn.com',
                     'iqiyi.com', 'ximalaya.com', 'ksapisrv.com', 'qpic.cn', 'bdurl.net', 'apple.com', 'sandai.net',
                     'sohu.com', 'heytapmobi.com', 'douyucdn.cn', 'pinduoduo.com', 'umeng.com', 'google.cn',
                     'byteimg.com', 'chinanetcenter.com', 'xiaomi.com', '163.com', 'gtimg.cn', 'funshion.com',
                     'amemv.com', 'qlogo.cn', 'yangkeduo.com', 'cuhk.edu.hk', 'redhat.com', 'bdstatic.com', 'ksyun.com',
                     'haplat.net', 'akamai.net', 'sina.cn', 'kugou.com', 'xiaomi.net', 'shifen.com', 'cibntv.net',
                     '51y5.net', 'akadns.net', 'kuaishou.com', 'huya.com', 'miui.com', 'qy.net', 'aliyun.com', 'wps.cn',
                     'alipay.com', 'onethingpcs.com', 'toutiao.com', 'kwimgs.com', 'gtimg.com', 'youku.com',
                     'windows.net', 'sinaimg.cn', 'weibo.com', 'myqcloud.com', 'uc.cn', 'cnzz.com', 'jpush.cn',
                     'mediav.com', 'cdnhwc2.com', 'mi.com', 'sogo.com', 'bcelive.com', 'bcebos.com', 'getui.com',
                     'coloros.com', 'qhimg.com', 'huawei.com', 'gstatic.com', 'nearme.com.cn', 'time.edu.cn', 'gitv.tv',
                     'msftncsi.com', 'kuaishouzt.com', 'mgtv.com', 'so.com', 'icloud.com', 'hwclouds-dns.com',
                     'netease.com', 'pddpic.com', 'iqiyipic.com', 'ctobsnssdk.com', 'mmstat.com', 'miaozhen.com',
                     'ys7.com', '360safe.com', 'qiyi.com', 'xunlei.com', 'mob.com', 'bilivideo.com', 'irs01.com',
                     'alibaba.com', 'bilibili.com', 'bdimg.com', 'kwaizt.com', 'meituan.com', 'heytapdownload.com',
                     '00cdn.com', 'qcloud.com', '21cn.com', 'wpscdn.cn', 'aaplimg.com', 'weibo.cn', 'tl88.net',
                     'bytecdn.cn', 'meizu.com', 'bytedance.com', 'sogoucdn.com', 'tendawifi.com', 'qchannel03.cn',
                     '1sapp.com', 'heytapimage.com', 'pingan.com', 'alibabadns.com', 'jomodns.com', '126.net',
                     'ucweb.com', 'baidupcs.com', 'windowsupdate.com', 'dbankcdn.com', 'googleapis.cn', '360buyimg.com',
                     'zhihu.com', 'dbankcloud.com', 'dbankcloud.eu', 'apple-dns.net', 'yy.com', 'openspeech.cn',
                     'baidustatic.com', 'duba.net', 'wsdvs.com', 'wscdns.com', 'xdrig.com', 'kuaishoupay.com', 'url.cn',
                     'hpplay.cn', 'cdngslb.com', 'facebook.com', 'tmall.com', 'kspkg.com', 'windows.com', 'tencent.com',
                     'www.googleapis.com', 'ludashi.com', 'bjshcw.com', 'easytomessage.com', 'jpush.io',
                     'alipayobjects.com', 'weiyun.com', 'sina.com', 'bsgslb.cn', 'live.com', 'xiaohongshu.com',
                     'kwaishop.com', 'pconline.com.cn', 'cytxl.com.cn', 'ks-cdn.com', 'ksmobile.com',
                     'drivergenius.com', 'lsttnews.com', 'smartont.net', 'ksyuncdn.com', 'byted.org', 'cmcm.com',
                     'dianping.com', 'ppstream.com', 'xuexi.cn', 'mwcname.com', 'safebrowsing.googleapis.com',
                     'alibabausercontent.com', 'alikunlun.com', 'idqqimg.com', 'avlyun.com', '58.com', 'tanx.com',
                     'miwifi.com', 'hdslb.com', 'tbcache.com', 'tencent-cloud.net', 'mediatek.com', 'kuwo.cn',
                     'tplinkcloud.com.cn', 'bytedance.net', '2345.com', 'lxdns.com', 'suning.com', 'hunantv.com',
                     'dui.ai', 'kwaicdn.com', 'hiido.com', 'meituan.net', 'atianqi.com', 'cmpassport.com',
                     'pinduoduo.net', 'gvt1.com', 'izatcloud.net', 'pingan.com.cn', 'teddymobile.cn', 'eastday.com',
                     'root-servers.net', 'biliapi.com', 'baofeng.net', 'clickwifi.net', 'xmcdn.com', 'digicert.com',
                     'cdn1218.com', 'tenpay.com', 'ugdtimg.com', 'qhres.com', 'aiclk.com', 'dbankcdn.cn', 'yahoo.com',
                     'nasa.gov', 'aliapp.org', 'peiluyou.com', 'play.googleapis.com', '360kuai.com', 'flash.cn',
                     'sankuai.com', 'ifeng.com', 'go2yd.com', 'nzwgs.com', 'cibn.cc', 'skysrt.com', 'hismarttv.com',
                     'lenovo.com.cn', 'cootekservice.com', 'bizport.cn', 'xunyou.mobi', 'dingtalk.com', 'nvidia.com',
                     'globalsign.com', 'bing.com', 'google-analytics.com', 'umengcloud.com', 'lianwangtech.com',
                     'akamaiedge.net', 'chinamobile.com', 'getui.net', 'cdnhwc1.com', 'myhwclouds.com', 'gdtimg.com',
                     'sm.cn', 'gepush.com', 'doubleclick.net', 'ksosoft.com', '189.cn', 'alikunlun.net', 'nzbdw.com',
                     'cloudlinks.cn', 'id6.me', 'lechange.cn', 'cmbchina.com', 'eastmoney.com', 'shuzilm.cn',
                     'wsglb0.com', 'weathercn.com', 'myapp.com', 'icbc.com.cn', 'vmall.com', 'cdn20.com', 'wifi188.com',
                     'chinacloudapi.cn', 'ksord.com', 'autonavi.com', 'didistatic.com', 'android.com', 'imtmp.net',
                     'pgzs.com', 'iotcplatform.com', '8686c.com', '360kan.com', 'ottcn.com', 'mzstatic.com', 'itc.cn',
                     'mozilla.com', 'icloud.com.cn', 'xiaojukeji.com', 'qutoutiao.net', 'dbankcloud.cn', 'wkanx.com',
                     'hao123.com', 'ijinshan.com', 'huoshan.com', 'cdntip.com', 'sinajs.cn', 'unity3d.com',
                     'googleusercontent.com', 'gcloudcs.com', 'clientservices.googleapis.com', 'bsgslb.com',
                     'tencentcs.com', 'fxltsbl.com', 'ykimg.com', 'bytedns.net', 'qhstatic.com', '1688.com',
                     'yfp2p.net', 'digitalassetlinks.googleapis.com', 'pglstatp-toutiao.com', '3322.org', 'ccb.com',
                     'qhupdate.com', 'abchina.com', 'app-measurement.com', 'cdntips.com', 'hitv.com', 'msstatic.com',
                     'omacloud.com', 'qihucdn.com', 'bd-caict.com', 'yidianzixun.com', 'dhrest.com', 'msn.com',
                     'riotgames.com', 'qiniudns.com', 'voiceads.cn', 'bdydns.com', '2345.cc', 'edgekey.net',
                     'miguvideo.com', 'kwaixiaodian.com', 'appsflyer.com', 'mini1.cn', 'netease.im', 'wanyol.com',
                     '12306.cn', 'wasu.tv', 'uczzd.cn', 'biliapi.net', 'asiyun.cn', 'wnsqzonebk.com', 'ctrip.com',
                     'immomo.com', 'reachmax.cn', 'zhimg.com', 'aisee.tv', 'xhscdn.com', 'gsxt.gov.cn', 'wostore.cn',
                     'douban.com', 'umsns.com', 'docer.com', '189cube.com', 'update.googleapis.com', 'mwcloudcdn.com',
                     'bytefcdn.com', 'wocloud.cn', 'pangolin-sdk-toutiao.com', 'qh-lb.com', 'xycloud.com', 'nist.gov',
                     'qhmsg.com', 'admaster.com.cn', 'znshuru.com', 'zjcdn.com', 'wtzw.com', 'oray.net', 'ele.me',
                     'cdn-go.cn', 'dnion.com', 'fenxi.com', 'tcdnlive.com', 'zhangyue.com', 'zwtianshangm.com',
                     '10086.cn', 'seetong.com', 'dnsv1.com', 'wegame.com.cn', 'weibocdn.com', 'android.googleapis.com',
                     'autohome.com.cn', 'dbankcloud.asia', 'voicecloud.cn', 'funshion.net', 'kdocs.cn',
                     'scorecardresearch.com', 'demonii.si', 'g.cn', 'tcloudfamily.com', 'qhimgs4.com', 'w3.org',
                     'bytecdn.com', '3.cn', 'upqzfile.com', 'myalicdn.com', 'qhimgs3.com', 'ruijie.com.cn',
                     'apple-dns.cn', 'douyin.com', 'sohu.com.cn', 'haosou.com', 'oa.com', 'flashapp.cn', 'ip138.com',
                     'networkbench.com', 'zhhainiao.com', 'pptv.com', 'teamviewer.com', 'igexin.com', 'mi-img.com',
                     'innotechx.com', 'gridsumdissector.com', 'xxx-tracker.com', 'darkorb.net', 'dbank.com', 'nyaa.uk',
                     'msftconnecttest.com', 'symcd.com', 'servicewechat.com', 'zuimeitianqi.com', 'cloudseetech.com',
                     'googlesyndication.com', 'vip.com', 'zuoyebang.cc', 'kgimg.com', 'ipinyou.com', 'bankcomm.com',
                     'gome.com.cn', 'boc.cn', 'googletagmanager.com', '10jqka.com.cn', 'cmvideo.cn', 'applk.cn',
                     'msedge.net', 'upeer.me', 'letv.com', 'wsds.cn', 'duowan.com', 'happyelements.cn', 'updrv.com',
                     'ireader.com', 'qhimgs0.com', 'fastdownload.xyz', 'tdnsv5.com', 'ffnews.cn', 'meitudata.com',
                     'miit.gov.cn', 'ctyunapi.cn', 'iesdouyin.com', 'qingting.fm', 'appjiagu.com', 'fengkongcloud.com',
                     'vivo.com', 'dftoutiao.com', 'tingmall.com', 'g9hc4.cn', 'skype.com', 'adobe.com', 'quickapp.cn',
                     '58cdn.com.cn', 'kaola.com', 'irs03.com', 'apple.cn', 'baidubce.com', 'ikuai8.com', 'intel.com',
                     'crashlytics.com', 'lenovomm.com', 'jdcloud.com', 'duba.com', 'huan.tv', 'cntv.cn', 'cdnhwc3.com',
                     'skyworthdigital.com', 'pkoplink.com', 'moji.com', 'xiongmaodaili.com', 'cebbank.com',
                     'hao123img.com', 'mcafee.com', 'wiwide.com', 'mercuryclouds.com.cn', 'fastweb.com.cn',
                     'youdao.com', 'acgvideo.com', 'ieeewifi.com', 'cpatrk.net']


def query_filter(query):
    query_list = query.split('.')
    if len(query_list) >= 4:
        return True
    else:
        return False


def topdomain_filter(query):
    cursor = 0
    query_list = []
    result = ''
    for i in range(len(query)):
        if (query[i] == '.'):
            query_list.append(query[cursor:i])
            cursor = i + 1
        elif (i == len(query) - 1):
            query_list.append(query[cursor:i + 1])
    if (len(query_list) == 0):
        result = 'NULL'
    elif (len(query_list) == 1):
        result = query_list[0]
    else:
        result = query_list[-2] + '.' + query_list[-1]

    return result


def three_topdomain_filter(query):
    cursor = 0
    query_list = []
    result = ''
    for i in range(len(query)):
        if (query[i] == '.'):
            query_list.append(query[cursor:i])
            cursor = i + 1
        elif (i == len(query) - 1):
            query_list.append(query[cursor:i + 1])
    if (len(query_list) == 0):
        result = 'NULL'
    elif (len(query_list) == 1):
        result = query_list[0]
    else:
        result = query_list[-3] + '.' + query_list[-2] + '.' + query_list[-1]

    return result


def subdomain_filter(query):
    cursor = 0
    query_list = []
    result = ''
    for i in range(len(query)):
        if (query[i] == '.'):
            query_list.append(query[cursor:i])
            cursor = i + 1
        elif (i == len(query) - 1):
            query_list.append(query[cursor:i + 1])
    if (len(query_list) == 0):
        result = 'NULL'
    elif (len(query_list) == 1):
        result = query_list[0]
    else:
        for i in range(len(query_list[:-3])):
            if i == len(query_list[:-3]) - 1:
                result = result + query_list[i]
            else:
                result = result + query_list[i] + '.'

    return result


def formatted_address_uid(json_line):
    result = json_line['id.orig_h'] + ' -> ' + json_line['id.resp_h'] + ':' + json_line['uid']
    return result


def tunnel_detecting(df, es, clf, corpus_dict):
    # 首先按照 orig_h -> resp_h 的形式将所有报文存入 host_formatted_query_dict
    host_formatted_dict = collections.defaultdict(list)
    for row in df.rdd.collect():
        data = row['value']
        source_log = json.loads(data)
        # 直接过滤掉标签数量低于4的域名
        if query_filter(source_log['query']):
            host_formatted_dict[formatted_address_uid(source_log)].append(source_log)

    formatted_dict = {}
    for key, source_log_list in host_formatted_dict.items():
        query_formatted_dict = collections.defaultdict(list)
        for source_log in source_log_list:
            try:
                three_topdomain = three_topdomain_filter(source_log['query'])
                query_formatted_dict[three_topdomain].append(source_log)
            except:
                pass
        formatted_dict[key] = query_formatted_dict

    filtered_formatted_dict = {}
    for key, source_log_dict in formatted_dict.items():
        filtered_query_formatted_dict = {}
        for three_topdomain, source_log_list in source_log_dict.items():
            # 进行一次白名单过滤
            try:
                if topdomain_filter(three_topdomain) not in domain_white_list and len(source_log_list) >= 100:
                    filtered_query_formatted_dict[three_topdomain] = source_log_list
            except:
                pass
        if filtered_query_formatted_dict:
            filtered_formatted_dict[key] = filtered_query_formatted_dict

    for key, source_log_dict in filtered_formatted_dict.items():
        for three_topdomain, source_log_list in source_log_dict.items():
            flag_list = []  # 将窗口范围内的报文的判断结果存入列表中
            for source_log in source_log_list:
                # 这里还未优化
                predict_result = detection_functions.detector(source_log, clf, corpus_dict)
                flag_list.append(predict_result)
            # 如果窗口内的阳性超过80%就判断此次窗口为一次隧道行为
            if flag_list.count(True) / len(flag_list) >= 0.8:
                for source_log in source_log_list:
                    # 将隧道窗口内的所有源报文都存入ES中
                    es.index(index="dns_tunnel_sourcelog", doc_type="sourcelogs", body=source_log)
                    predict_result = detection_functions.detector(source_log, clf, corpus_dict)
                    # 以本次窗口内第一条检测为True的报文作为代表生成alter_log存入dns_anomaly_event
                    if predict_result == True:
                        alarm_log = detection_functions.alarmWirter(source_log)
                        # 将alter_log存入ES中
                        es.index(index="dns_anomaly_event", doc_type="_doc", body=alarm_log)

                        f = open('/opt/app/jxlu/DNSTunnelDetecting/data/alarmlog.log', mode='a')
                        f.write('-------------------------------------\n')
                        f.write(str(alarm_log) + '\n' + str(source_log) + '\n')
                        f.write('-------------------------------------\n')
                        f.close()

                        break

    with open('/opt/app/jxlu/DNSTunnelDetecting/data/test.log', 'a') as f:
        f.write('----------------------------\n')
        f.write(str(filtered_formatted_dict) + '\n')
        f.write('----------------------------\n')


def foreach_batch_function(df, epoch_id):
    with open('/opt/app/jxlu/DNSTunnelDetecting/data/corpus_dict_2021_10_06_16_36_43.json', 'r') as f:
        corpus_dict = json.load(f)
    clf = joblib.load('/opt/app/jxlu/DNSTunnelDetecting/rule_files/tree_14781011_4.pkl')
    es = Elasticsearch(
        ['hadoop122:9200']
    )
    df.persist()
    tunnel_detecting(df, es, clf, corpus_dict)
    df.unpersist()

    es.close()


# 任务代码
if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .appName("dnstunnel_detection_app") \
        .getOrCreate()

    spark.sparkContext.addPyFile('/opt/app/jxlu/DNSTunnelDetecting/detection_functions.py')
    spark.sparkContext.addPyFile('/opt/app/jxlu/DNSTunnelDetecting/rule_files/tree_14781011_4.pkl')
    spark.sparkContext.addPyFile('/opt/app/jxlu/DNSTunnelDetecting/data/corpus_dict_2021_10_06_16_36_43.json')

    spark.sparkContext.setLogLevel("WARN")

    streamingDF = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", 'hadoop122:9092') \
        .option('subscribe', 'dnslogs') \
        .load() \
        .selectExpr("CAST(value AS STRING)")

    # ProcessingTime trigger with two-seconds micro-batch interval
    query = streamingDF.writeStream \
        .foreachBatch(foreach_batch_function) \
        .trigger(processingTime='5 seconds') \
        .start()
    #        .trigger(processingTime='3.5 seconds') \

    query.awaitTermination()
