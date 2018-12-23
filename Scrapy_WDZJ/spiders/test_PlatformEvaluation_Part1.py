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


class PlatformEvaluation_Part1Spider(scrapy.Spider):
    name = 'PlatformEvaluation_Part1'
    start_urls = ['https://www.wdzj.com/dangan/search?filter']

    def parse(self, response):
        """

        :param response:
        :return:
        """
        for platId in range(40,60):
            url = 'https://www.wdzj.com/plat-center/platReview/getPlatReviewList?platId={0}&currentPage=1&pageSize=2&orderType=0'.format(platId)
            yield Request(url, meta={'platId': platId}, callback=self.parse_PlatformEvaluation_Part1)


    def parse_PlatformEvaluation_Part1(self, response):
        j_data = json.loads(response.body)
        item = PlatformEvaluation_Part1()
        item['platId'] = response.meta['platId']
        item['good'] = j_data['data']['platReviewEvaluation']['good']
        item['bad'] = j_data['data']['platReviewEvaluation']['bad']
        item['midd'] = j_data['data']['platReviewEvaluation']['midd']
        yield item


