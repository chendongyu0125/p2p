# -*- coding: utf-8 -*-

import scrapy
from scrapy.http import Request, FormRequest
from Scrapy_WDZJ.items import *
import datetime
from Scrapy_WDZJ.tools.strtools import *
import requests
from lxml import etree
import json
import math
import re
from scrapy.loader import ItemLoader
import time
import logging
from Scrapy_WDZJ.tools.net import *
from Scrapy_WDZJ import settings

class InitalizeSpider(scrapy.Spider):
    name = 'DailyOperation_Part3'
    start_urls = ['https://www.wdzj.com/dangan/search?filter']

    def parse(self, response):
        """

        :param response:
        :return:
        """
        # platId = 40
        for platId in range(1, 6000):
            url = 'https://shuju.wdzj.com/plat-info-target.html'
            post_data = {"wdzjPlatId": str(platId), "type": "1", "target1": "7", "target2": "8"}
            yield FormRequest(url, formdata=post_data, meta={"platId": str(platId), "tries":str(0)},
                              callback=self.parse_DailyOperation_Part3)

    def parse_DailyOperation_Part3(self, response):
        """

        :param response:
        :return:
        """
        if response.status == 555:
            platId = response.meta['platId']
            post_data = {"wdzjPlatId": str(platId), "type": "1", "target1": "7", "target2": "8"}
            meta=response.meta
            tries = int(meta['tries'])
            meta['tries']=str(tries+1)
            reconnect_FormRequest(response=response, meta=meta, callback=self.parse_DailyOperation_Part3, formdata=post_data)
        else:
            j_data = json.loads(response.body)
            platId = response.meta['platId']
            date_list = j_data['date']
            d1_list = j_data['data1']
            d2_list = j_data['data2']
            for (d, d1, d2) in zip(date_list, d1_list, d2_list):
                item = DailyOperation_Part3()
                item.initValue()
                item['platId']=platId
                item['date']=d
                item['bidValuePerCapita']=d1
                item['borValuePerCapita']=d2
                item['CollectionTime']=get_time()
                yield item

