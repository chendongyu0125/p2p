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


class GongshangSpider(scrapy.Spider):
    name = 'gongshang'

    start_urls = ['https://www.wdzj.com/dangan/search?filter&currentPage={0}'.format(page) for page in range(20,30)]

    def parse(self, response):
        """

        :param response:
        :return:
        """
        # platId = 40
        xpathRoot = '//ul[@class="terraceList"]/li[@class="item"]'
        leaf = './/div[@class="itemTitle"]/h2/a/@href'
        baseURL = "https://www.wdzj.com"
        selectors = response.xpath(xpathRoot)
        for selector in selectors:
            url = selector.xpath(leaf).extract()[0]
            plat_url=baseURL+url
            plat_gongshang_url=plat_url+"gongshang/"
            yield Request(plat_gongshang_url, callback=self.parse_gongshang)




    def parse_gongshang(self, response):
        """
        scrapy parse --callback=parse_gongshang "https://www.wdzj.com/dangan/wlb/gongshang/"
        测试网页：
        "https://www.wdzj.com/dangan/wlb/gongshang/"
        "https://www.wdzj.com/dangan/hxd5/gongshang/"
        "https://www.wdzj.com/dangan/hzed/gongshang/" （含公司变更信息）
        "https://www.wdzj.com/dangan/edsqb/gongshang/"
        测试代码：scrapy parse --callback=parse_gongshang


        :param response:
        :return:
        """
        platId = response.xpath('//input[@id="platId"]/@value').extract()[0]
        # (1) Crawl AbnormalOperationHistory
        xpath_root = '//div[text()="异常经营"]/..//table[@class="table-ic"]/tbody[@class="tbody"]/tr'
        selectors = response.xpath(xpath_root)
        var_list = [
            'SerialNumber',
            'InListReason',
            'InlistDate',
            'InListInstitution',
            'RemoveListReason',
            'RemoveListDate',
            'RemoveListInstitution',
        ]
        xpath_leaf_list = [
            './td[1]/text()',
            './td[2]/text()',
            './td[3]/text()',
            './td[4]/text()',
            './td[5]/text()',
            './td[6]/text()',
            './td[7]/text()',
        ]
        for selector in selectors:
            item = AbnormalOperationHistory()
            item.initValue()
            item['platId'] = platId
            item_loader = WDZJItemLoader(item=item, selector=selector)
            for (var, xpath) in zip(var_list, xpath_leaf_list):
                item_loader.add_xpath(var, xpath, MapCompose(clean_values))
            item = item_loader.load_item()
            item['CollectionTime'] = get_time()
            yield item

        # (2) Crawl CapitalStructure
        # 股权信息
        xpath_root = '//div[@id="gqInfoBox"]//table[@class="table-ic"]/tbody[@class="tbody"]/tr'
        selectors = response.xpath(xpath_root)
        xpath_leaf_list = [
            './td[1]/text()',
            './td[2]/text()',
            './td[3]/text()',
        ]
        var_list = [
            'ShareholderName',
            'SharePercent',
            'SubscribedCapitalContribution',
        ]
        for selector in selectors:
            item = CapitalStructure()
            item.initValue()
            item_loader = WDZJItemLoader(item=item, selector=selector)
            for (var, xpath) in zip(var_list, xpath_leaf_list):
                item_loader.add_xpath(var, xpath)
            item = item_loader.load_item()
            item['platId'] = platId
            item['CollectionTime'] = get_time()
            yield item

        # （3）Crawl CompanyBackground_Part1
        xpath_list = [
            '//input[@id="platId"]/@value',
            '//td[text()="登记状态"]/../td[2]/text()',
            '//td[text()="登记机关"]/../td[2]/text()',
            '//td[text()="备案域名"]/../td[2]/text()',
            '//td[text()="备案单位名称"]/../td[2]/text()',
            '//td[text()="开业日期"]/../td[4]/text()',
            '//td[text()="营业日期"]/../td[4]/text()',
            '//td[text()="核准日期"]/../td[4]/text()',
            '//td[text()="备案时间"]/../td[4]/text()',
            '//td[text()="备案单位性质"]/../td[4]/text()',
            '//td[text()="经营范围"]/../td[2]/p/text()',
            '//td[text()="注册资本"]/../td[4]/text()',
            '//td[text()="统一社会信用代码"]/../td[4]/text()',
        ]
        var_list = [
            'platId',
            'RegisterStatus',
            'RegisterInstitution',
            'DomainName',
            'RegisterCompanyName',
            'OpenDate',
            'RunningDate',
            'ApprovedDate',
            'PutInRecordDate',
            'CompanyNature',
            'BusinessScope',
            'RegisterCapital',
            'UnifiedSocialCreditNumber',
        ]
        item = CompanyBackground_Part1()
        item.initValue()
        item_loader = WDZJItemLoader(item=item, response=response)
        for (var, xpath) in zip(var_list, xpath_list):
            item_loader.add_xpath(var, xpath, MapCompose(clean_values))
        item = item_loader.load_item()
        yield item

        # (4) Crawl CompanyChangeHistory
        xpath_root = '//div[text()="变更记录"]/..//table[@class="table-ic"]/tbody[@class="tbody"]/tr'
        selectors = response.xpath(xpath_root)
        var_list = [
            'ChangeDate',
            'ChangeType',
            'InfoBeforeChange',
            'InfoAfterChange',
        ]
        xpath_leaf_list = [

            './td[1]/text()',
            './td[2]/text()',
            './td[3]/text()',
            './td[4]/text()',
        ]
        for selector in selectors:
            item = CompanyChangeHistory()
            item.initValue()
            item_loader = WDZJItemLoader(item=item, selector=selector)
            for (var, xpath) in zip(var_list, xpath_leaf_list):
                item_loader.add_xpath(var, xpath, MapCompose(clean_values))
            item = item_loader.load_item()
            item['platId'] = platId
            item['CollectionTime'] = get_time()
            yield item


