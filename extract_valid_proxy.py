# coding=utf-8
import sqlite3
import pandas as pd
import requests
import logging
from Scrapy_WDZJ.tools.net import *
import datetime
logging.basicConfig(level=logging.DEBUG)

sql = "select ip,port,protocal from ValidProxy"
conn = sqlite3.connect('scrapy_wdzj2018-12-23.db')
ips = pd.read_sql(sql=sql, con=conn)
logging.debug(ips)
valid_ip_table=[]
for index, row in ips.iterrows():
    valid_ip=valid_proxyip(row)
    if valid_ip['valid']==1:
        valid_ip['valid_ip_url']="{0}://{1}:{2}".format(valid_ip['protocal'], valid_ip['ip'], valid_ip['port'])
        logging.debug(valid_ip)
        valid_ip_table.append(valid_ip)

df=pd.DataFrame(valid_ip_table)
timestamp = str(datetime.datetime.now().strftime("%Y-%m-%d"))
df.to_csv("valid_ip_table{0}.csv".format(timestamp))