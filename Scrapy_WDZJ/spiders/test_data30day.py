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

class InitalizeSpider(scrapy.Spider):
    name = 'data30day'
    start_urls = ['https://www.wdzj.com/dangan/search?filter']

    def parse(self, response):
        """

        :param response:
        :return:
        """
        # platId = 40
        for platId in range(1, 6000):
            url = 'https://wwwservice.wdzj.com/api/plat/platData30Days?platId={0}'.format(platId)
            if response.status == 555:
                reconnect_Request(response=response, callback=self.parse)
            else:
                yield Request(url, callback=self.parse_data30day)


    def parse_data30day(self,response):
        """
        测试网页：
        https://wwwservice.wdzj.com/api/plat/platData30Days?platId=40
        https://wwwservice.wdzj.com/api/plat/platData30Days?platId=5216
        :param response:
        :return:
        """
        if response.status == 555:
            reconnect_Request(response=response, callback=self.parse_data30day)
        else:
            j_data = json.loads(response.body.decode("UTF-8"))
            if j_data['data']['platOuterVo'] == "":
                return
            # (1) Crawl PlatformDetail
            var_list = [
                'platId',
                'platCode',
                'platNamePin',
                'platName',
                'platBackgroundDetailExpand',
                'platStatus',
                'problemTime',
                'onlineDate',
                'locationArea',
                'locationAreaName',
                'locationCity',
                'locationCityName',
                'registeredCapital',
                'bankFunds',
                'bankCapital',
                'association',
                'associationDetail',
                'autoBid',
                'newTrustCreditor',
                'bidSecurity',
                'securityModel',
                'securityModelCode',
                'securityModelOther',
                'companyName',
                'juridicalPerson',
                'businessType',
                'officeAddress',
                'recordId',
                'oldPlatName',
                'actualCapital',
                'manageExpense',
                'manageExpenseDetail',
                'withdrawExpense',
                'withdrawExpenseDetail',
                'rechargeExpense',
                'rechargeExpenseDetail',
                'transferExpense',
                'transferExpenseDetail',
                'vipExpense',
                'vipExpenseDetail',
                'serviceTel',
                'platBackgroundExpandChild',
                'servicePhone',
                'equityVoList',
                'recordLicId',
                'riskcontrolDetail',
                'riskcontrolDetailArray',
                'payment',
                'paymode',
                'riskcontrol',
                'credit',
                'inspection',
                'problem',
                'platEquity',
                'platIsStatic',
                'displayFlg',
                'bindingFlag',
                'showShuju',
                'tzjPj',
                'gjlhhTime',
                'withTzj',
                'gjlhhFlag',
                'platBackground',
                'platBackgroundExpand',
                'platBackgroundDetail',
                'platBackgroundMark',
                'riskFunds',
                'riskCapital',
                'fundCapital',
                'trustFunds',
                'trustCapital',
                'trustCreditor',
                'riskReserve',
                'trustCreditorMonth',
                'gruarantee',

            ]
            item = PlatformDetail()
            item.initValue()
            item_loader = WDZJItemLoader(item=item, response=response)
            for var in var_list:
                item_loader.add_value(var, j_data['data']['platOuterVo'][var], MapCompose(clean_values))
            item = item_loader.load_item()
            item['CollectionTime'] = get_time()
            yield item

            # (2) Crawl PlatformEvaluation_Part2
            varlist = [
                'platId',
                'drawScore',
                'delayScore',
                'serviceScore',
                'experienceScore',
                'drawScoreDetail',
                'delayScoreDetail',
                'serviceScoreDetail',
                'experienceScoreDetail',
                'CollectionTime',
            ]
            item = PlatformEvaluation_Part2()
            item.initValue()
            for var in varlist[:-1]:
                item[var] = j_data['data']['platOuterVo'][var]
            item['CollectionTime'] = get_time()
            yield item

            # (3) Crawl OperationPerformanceMonthly
            if 'platShujuMap' in j_data['data'].keys():
                item = OperationPerformanceMonthly()
                item.initValue()
                item['platId'] = j_data['data']['platOuterVo']['platId']
                item['amount'] = j_data['data']['platShujuMap']['amount']
                item['bidder_num'] = j_data['data']['platShujuMap']['bidder_num']
                item['borrower_num'] = j_data['data']['platShujuMap']['borrower_num']
                item['bor_top10'] = j_data['data']['platShujuMap']['bor_top10']
                item['bid_top10'] = j_data['data']['platShujuMap']['bid_top10']
                item['net_inflow'] = j_data['data']['platShujuMap']['net_inflow']
                item['amount_percent'] = j_data['data']['platShujuMap']['amount_percent']
                item['bidder_num_percent'] = j_data['data']['platShujuMap']['bidder_num_percent']
                item['borrower_num_percent'] = j_data['data']['platShujuMap']['borrower_num_percent']
                item['bor_top10_change'] = j_data['data']['platShujuMap']['bor_top10_change']
                item['bid_top10_change'] = j_data['data']['platShujuMap']['bid_top10_change']
                item['net_inflow_percent'] = j_data['data']['platShujuMap']['net_inflow_percent']
                item['CollectionTime']=get_time()
                yield item


