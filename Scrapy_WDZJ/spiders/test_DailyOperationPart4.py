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


class InitalizeSpider(scrapy.Spider):
    name = 'DailyOperation_Part4'
    start_urls = ['https://www.wdzj.com/dangan/search?filter']

    def parse(self, response):
        """

        :param response:
        :return:
        """
        # platId = 40
        for platId in range(40, 60):
            url = 'https://shuju.wdzj.com/plat-info-target.html'
            post_data = {"wdzjPlatId": str(platId), "type": "1", "target1": "19", "target2": "20"}
            yield FormRequest(url, formdata=post_data, meta={"platId": str(platId)},
                              callback=self.parse_DailyOperation_Part4)

    def parse_DailyOperation_Part4(self, response):
        j_data = json.loads(response.body)
        platId = response.meta['platId']
        date_list = j_data['date']
        d1_list = j_data['data1']
        d2_list = j_data['data2']
        for (d, d1, d2) in zip(date_list, d1_list, d2_list):
            item = DailyOperation_Part4()
            item.initValue()
            item['platId'] = platId
            item['date'] = d
            item['newBidders'] = d1
            item['oldBidders'] = d2
            item['CollectionTime'] = get_time()
            yield item

