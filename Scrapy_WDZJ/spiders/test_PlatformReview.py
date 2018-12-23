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


class UnitSpider(scrapy.Spider):
    name = 'unit'
    # allowed_domains = ['wdzj']

    start_urls = ['https://www.wdzj.com/plat-center/platReview/getPlatReviewList?platId=40&currentPage=2&pageSize=20&orderType=0']

    def start_requests(self):
        """
        :return:
        """
        platId = '40'

        url = 'https://www.wdzj.com/plat-center/platReview/getPlatReviewList?platId={0}&currentPage=1&pageSize=20&orderType=0'.format(platId)

        logging.info(url)
        return [
            Request(url, meta={'tries': 0}),
        ]

    def parse(self, response):
        res_txt = response.body.decode("UTF-8")
        logging.info(response.body.decode("UTF-8"))
        j_data = json.loads(res_txt)
        if j_data['message'] == '访问次数过于频繁' and int(response.meta['tries']) < 100:
            tries = int(response.meta['tries']) + 1
            logging.info("访问次数过于频繁，休息一下")
            time.sleep(8*tries)
            tries=tries+1
            yield Request(response.url, meta={'tries': tries}, dont_filter=True, callback=self.parse)
        else:
            try:
                platId = j_data['data']['platReviewSearchVo']['platId']
                total_page = int(j_data['data']['pagination']['totalPage'])
                print(total_page)
                current_page = int(j_data['data']['pagination']['currentPage'])
                print(current_page)
                if current_page < total_page:
                    next_url = 'https://www.wdzj.com/plat-center/platReview/getPlatReviewList?platId={0}&currentPage={1}&pageSize=20&orderType=0'.format(
                        platId, current_page + 1)
                    print(next_url)
                    yield Request(next_url, meta={'tries': 0}, dont_filter=True, callback=self.parse)

                review_list = j_data['data']['pagination']['list']
                for review in review_list:
                    if 'replyList' in review.keys():
                        for reply in review['replyList']:
                            item=load_item(reply, PlatformReviewReply())
                            yield item
                        review.pop('replyList')
                        review['hasReply'] = 1
                    item=load_item(review, PlatformReview())
                    yield item
            except Exception as err:
                logging.info(err)


