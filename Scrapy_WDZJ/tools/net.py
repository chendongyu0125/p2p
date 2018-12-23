# coding=utf-8
import urllib.request
from scrapy.http import Request, FormRequest
import time
from Scrapy_WDZJ import settings
import logging
import requests


def valid_proxyip(proxy):
    """
    proxy['ip'] = '182.18.13.149'
    proxy['port'] = '53281'
    proxy['protocal'] = 'http'

    :param proxy:
    :return: True if it is a valid proxy ip
    """

    # telnetlib.Telnet(ip, port=port, timeout=3)
    ip = proxy['ip']
    port = proxy['port']
    protocal = proxy['protocal']
    headers = {
        'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36"
    }
    proxies = {protocal: "{0}://{1}:{2}".format(protocal, ip, port)}
    logging.debug(proxies)
    try:
        res=requests.get('http://www.suda.edu.cn', proxies=proxies, headers=headers, timeout=1)
        if res.status_code==200:
            proxy['valid']=1
        else:
            proxy['valid']=0
    except Exception as err:
        proxy['valid']=0
        logging.debug(err)
    return proxy

def reconnect_Request(response, callback):
    """
    如果访问被拒，返回的状态码是555，含义为疑似遭受黑客攻击
    休息时间 settings.get("SLEEP_UNIT") * tries
    如果尝试次数小于100，则继续尝试
    :param response: 放回状态
    :param callback: 再次尝试的请求
    :return: Request | None
    """
    url = response.url
    meta = response.meta
    tries = int(meta['tries'])
    meta['tries'] =str(tries + 1)
    if tries <= 100:

        sleep_unit= int(settings.SLEEP_UNIT)
        logging.debug("尝试通过Request方式连接:{0}, meta参数:{1},尝试次数:{2}，休息一下".format(url, meta, tries))
        time.sleep(sleep_unit*tries)
        return Request(url, meta=meta, callback=callback)
    else:
        logging.error("尝试通过Request方式连接:{0}, meta参数:{1}, 尝试次数大于:100， 放弃。。。".format(url, meta))


def reconnect_FormRequest(response, callback, formdata):
    """
    如果访问被拒，返回的状态码是555
    休息时间 settings.get("SLEEP_UNIT") * tries
    如果尝试次数小于100，则继续尝试
    :param response: 放回状态
    :param callback: 再次尝试的请求
    :return: FormRequest | None
    """

    url = response.url
    meta = response.meta
    tries = int(meta['tries'])
    meta['tries'] =str(tries + 1)

    if tries <= 100:

        sleep_unit= int(settings.SLEEP_UNIT)
        logging.debug("尝试通过FormRequest方式连接:{0}, formdata参数:{1}, meta参数:{2},尝试次数:{3}，休息一下".format(url, formdata, meta, tries))
        time.sleep(sleep_unit*tries)
        return FormRequest(url, formdata=formdata, meta=meta, callback=callback)
    else:
        logging.error("尝试通过FormRequest方式连接:{0}, formdata参数:{1}, meta参数:{2}, 尝试次数:大于100， 放弃。。。".format(url, meta))