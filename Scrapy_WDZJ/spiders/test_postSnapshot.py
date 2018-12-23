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
    name = 'postSnapshot'
    start_urls = ['https://www.wdzj.com/dangan/search?filter']

    def parse(self, response):
        """

        :param response:
        :return:
        """
        # platId = 40
        for platId in range(40, 42):
            url = 'https://www.wdzj.com/front/bbsInfo/{0}/1000000/1'.format(platId)  # 1000000-每页显示评论数，1-当前页数
            yield Request(url, meta={"platId": platId}, callback=self.parse_PostSnapshot)

    def parse_PostSnapshot(self, response):
        """
        test instance:
        scrapy parse --callback=parse_PostSnapshot "https://www.wdzj.com/front/bbsInfo/40/1000000/1"

        :param response:
        :return:
        """
        platId = response.meta['platId']
        xpath_root = "//ul/li"
        selectors = response.xpath(xpath_root)
        varlist = [

            'AuthorID',
            'tid',
            'author_name',
            'terminal',
            'PostingDate',
            'PostAbstract',
            'PostTitle',
        ]
        xpath_leaf_list =[

            './/div[@class="lbox"]//a[contains(@onclick,"personalCenter")]/@onclick',
            './/div[@class="tit"]/a/@href',
            './/div[@class="lbox"]//a[contains(@onclick,"personalCenter")]/text()',
            './/div[@class="userxx"]/div[@class="lbox"]/span[1]/text()',
            './/div[@class="userxx"]/div[@class="lbox"]/span[2]/text()',
            './/div[@class="cen"]/a/text()',
            './/div[@class="tit"]/a/text()',
        ]
        xpath_pair = dict(zip(varlist, xpath_leaf_list))
        for selector in selectors:
            item = PostSnapshot()
            # item.initValue()
            item['platId']=platId
            item_loader = WDZJItemLoader(item=item, selector=selector)
            item_loader.add_xpath('tid', xpath_pair['tid'], MapCompose(parse_digits))
            item_loader.add_xpath('AuthorID', xpath_pair['AuthorID'], MapCompose(parse_digits))
            for (var, xpath) in zip(varlist, xpath_leaf_list):
                item_loader.add_xpath(var, xpath, MapCompose(clean_values))
            item = item_loader.load_item()
            item['CollectionTime'] = get_time()

            tid = item['tid']
            url = 'https://bbs.wdzj.com/thread-{0}-1-1.html'.format(tid)
            yield Request(url, meta={"tid": tid}, callback=self.parse_PostDetail)
            url = 'https://bbs.wdzj.com/thread/getAuthorComment?type=1&author=&tid={0}&page=1&page_size=50'.format(tid)
            yield Request(url, meta={"tid": tid, "current_page":1}, callback=self.parse_PostReply)
            yield item

    def parse_PostDetail(self, response):
        varlist = [
            'tid',
            'AuthorID',
            'PostTitle',
            'PostingDate',
            'Content',
            'NumberOfComments',
            'Recommend_Yes',
            'Recommend_No',
            'NumberOfReaders',
            'PostType',
            'CollectionTime',
        ]
        xpath_list = [
            '//span[@data-authorid]/@data-authorid',
            '//h1[@class="context-title"]/text()',
            '//div[@class="post-time"]/span/text()',
            'string(//div[@class="post-inner-txt"]/div[@class="news_con_p"])',
            '//span[@class="replyCount"]/text()',
            '//span[@id="recommend"]/text()',
            '//a[@hates]/span/text()',
            '//p[text()="阅读量"]/../p[1]/text()',
            '//div[@class="commun-des"]/text()',
        ]
        tid = response.meta['tid']
        item = PostDetail()
        item.initValue()
        item_loader = WDZJItemLoader(item=item, response=response)
        item_loader.add_value('tid', tid)
        for (var, xpath) in zip(varlist[1:-1], xpath_list):
            item_loader.add_xpath(var, xpath, MapCompose(clean_values))
        item = item_loader.load_item()
        item['CollectionTime']=get_time()
        yield item


    def parse_PostReply(self, response):
        varlist = [
            'id',
            'tid',
            'author_id',
            'author_name',
            'terminal',
            'message',
            'create_time',
            'support',
            'fid',
            'parent_uid',
            'path',
            'is_top',
            'position',
            'attachment',
            'CollectionTime',
        ]
        tid = response.meta['tid']
        j_data = json.loads(response.body)
        comment_total=int(j_data['commentTotal'])
        current_page = int(response.meta['current_page'])
        pages_total = math.ceil(comment_total/50)
        if current_page < pages_total: #爬取更多页面
            next_page = current_page+1
            url = 'https://bbs.wdzj.com/thread/getAuthorComment?type=1&author=&tid={0}&page={1}&page_size=50'.format(tid, next_page)
            yield Request(url, meta={"tid": tid, "current_page": next_page})

        # 抓取当前页面内容
        comments = j_data['comment']
        for comment in comments:
            item = PostReply()
            item.initValue()
            item_loader = WDZJItemLoader(item=item, response=response)
            for var in varlist[:-1]:
                item_loader.add_value(var, comment[var], MapCompose(clean_values, remove_slash))
            item = item_loader.load_item()
            msg = item['message']
            selector = etree.HTML(msg)
            msg=selector.xpath("string(.)")
            item['message']=clean_values(msg)
            yield item