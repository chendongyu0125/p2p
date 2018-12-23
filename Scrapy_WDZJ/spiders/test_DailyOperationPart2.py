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
    name = 'DailyOperation_Part2'
    start_urls = ['https://www.wdzj.com/dangan/search?filter']

    def parse(self, response):
        """

        :param response:
        :return:
        """
        # platId = 40
        for platId in range(40, 60):
            url = 'https://shuju.wdzj.com/wdzj-archives-chart.html?wdzjPlatId={0}&type=0&status=0'.format(platId)
            yield Request(url, callback=self.parse_DailyOperation_Part2)


    def parse_DailyOperation_Part2(self, response):
        j_data = json.loads(response.body)
        if 'x' not in j_data.keys():
            return
        platId = j_data['wdzjPlatId']
        date_list = j_data['x']
        platformReturn_list = j_data['y1']
        amountValue_list = j_data['y2']
        for (date, platformReturn, amountValue) in zip(date_list, platformReturn_list, amountValue_list):
            item = DailyOperation_Part2()
            item.initValue()
            item['platId']=platId
            item['date']=date
            item['platformReturn']=platformReturn
            item['amountValue']=amountValue
            item['CollectionTime']=get_time()
            yield item


