from sqlalchemy import create_engine
from clickhouse_driver import Client
import pandas as pd
import multiprocessing
import datetime
import time
import json
import re

user ='fanhaojie'
passwd ='Chenfan@123'
host ='10.228.86.203'
port ='11101'
dbname1 = 'test'
engine2 = create_engine("mysql+pymysql://%s:%s@%s:%s/%s?charset=utf8"%(user,passwd,host,port,dbname1))

start_time = datetime.datetime.now()
client2 = Client(host='10.228.86.203', user='default', database='weibo_his', password='nEB7+b3X')

def write_queue(queue):
    print('开始读取数据')
    IMAGE_URL = pd.read_csv(r'all_images1.csv', encoding='gbk')
    
    # 循环写入数
    # data = pd.read_excel('comments_sina.xlsx')
    data_list = [IMAGE_URL.ix[i].to_dict() for i in IMAGE_URL.index.values]
    for i in data_list:
        if queue.full():
            print("队列已满!")
        queue.put(i)


def read_queue(queue, pid):
    # 循环读取队列消息
    count = 1
    print("开始下载图片")
    while True:

        result = queue.get()
        try:

            b = str(result['taobao_goods_id'])
            import requests
            re_images = re.sub('https:|http:','',result['images_url_04'])
            r = requests.get('http:' + re_images)
            aa_code = r.status_code
            print('第一次请求',r)
            if aa_code==404:
                dict_found = [{"taobao_goods_id":result['taobao_goods_id'],"images_url_04":re_images}]
                data_code_ = pd.DataFrame(dict_found)
                pd.io.sql.to_sql(data_code_, 'images_down_shibai', engine2, schema='test', if_exists='append', index=False)
            path = "all_goods_info_images1/"+b+'.jpg'
            with open(path, 'wb') as f:
                f.write(r.content)
                f.close()
        except:
            recf_images = re.sub('https:|http:', '', result['images_url_04'])
            r_a = requests.get('http:' + recf_images)
            vvvv = r_a.status_code
            print('第二次请求',r_a)
            b_goods_id = str(result['taobao_goods_id'])
            path = "all_goods_info_images1/"+b_goods_id+'.jpg'
            with open(path, 'wb') as f:
                f.write(r_a.content)
                f.close()
            if vvvv!=200:
                dict_v = [{"taobao_goods_id":result['taobao_goods_id'],"image_url":recf_images}]
                data_v = pd.DataFrame(dict_v)
                pd.io.sql.to_sql(data_v, 'images_down_shibai_v', engine2, schema='test', if_exists='append', index=False)
if __name__ == '__main__':
    count = 1
    # 创建消息队列
    queue = multiprocessing.Queue(200)
    # 创建子进程
    p1 = multiprocessing.Process(target=write_queue, args=(queue,))
    # 等待p1写数据进程执行结束后，再往下执行
    p2 = multiprocessing.Process(target=read_queue, args=(queue, 1))
    p3 = multiprocessing.Process(target=read_queue, args=(queue, 2))
    p4 = multiprocessing.Process(target=read_queue, args=(queue, 3))
    p5 = multiprocessing.Process(target=read_queue, args=(queue, 4))
    p6 = multiprocessing.Process(target=read_queue, args=(queue, 5))
    p7 = multiprocessing.Process(target=read_queue, args=(queue, 6))
    p8 = multiprocessing.Process(target=read_queue, args=(queue, 7))
    p9 = multiprocessing.Process(target=read_queue, args=(queue, 8))
    p10 = multiprocessing.Process(target=read_queue, args=(queue, 9))
    p11 = multiprocessing.Process(target=read_queue, args=(queue, 10))
    p12 = multiprocessing.Process(target=read_queue, args=(queue, 11))
    p13 = multiprocessing.Process(target=read_queue, args=(queue, 12))
    p14 = multiprocessing.Process(target=read_queue, args=(queue, 13))
    p15 = multiprocessing.Process(target=read_queue, args=(queue, 14))
    p16 = multiprocessing.Process(target=read_queue, args=(queue, 15))
    p17 = multiprocessing.Process(target=read_queue, args=(queue, 16))
    p1.start()
    p2.start()
    p3.start()
    p4.start()
    p5.start()
    p6.start()
    p7.start()
    p8.start()
    p9.start()
    p10.start()
    p11.start()
    p12.start()
    p13.start()
    p14.start()
    p15.start()
    p16.start()
    p17.start()
    p1.join()
    p2.join()
    p3.join()
    print("=============")
    p4.join()
    p5.join()
    p6.join()
    p7.join()
    p8.join()
    p9.join()
    p10.join()
    p11.join()
    p12.join()
    p13.join()
    p14.join()
    p15.join()
    p16.join()
    p17.join()
