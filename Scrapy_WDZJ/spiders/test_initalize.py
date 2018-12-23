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
    name = 'initalize'
    start_urls = ['https://www.wdzj.com/dangan/search?filter']

    def parse(self, response):
        """

        :param response:
        :return:
        """
        platId = '40'
        url ='https://shuju.wdzj.com/plat-info-initialize.html'
        for i in range(1):
            for platId in range(1,6000):
                post_data = {"wdzjPlatId": str(platId)}
                yield FormRequest(url, meta={'tries': 0}, formdata=post_data, callback=self.parse_initalize, dont_filter=True)


    def parse_initalize(self, response):
        """
        (1) Crawl DailyOperation_Part1
        (2) Crawl OperationPerformanceDaily
        :param response:
        :return:
        """
        # logging.info(response.status)
        # if int(response.status) == 555:
        #     logging.info("正文文本"+response.body.decode("UTF-8"))
        #     tries = int(response.meta['tries'])
        #     tries += 1
        #     time.sleep(int(self.settings.get('SLEEP_UNIT')) * tries)
        #     yield Request(response.url, meta={'tries': tries}, dont_filter=True, callback=self.parse_initalize)


        tries = int(response.meta['tries'])
        if response.status == 555 and tries <100:
            tries+=1
            time.sleep(int(self.settings.get('SLEEP_UNIT')) * tries)
            yield Request(response.url, meta={'tries': tries}, dont_filter=True, callback=self.parse_initalize)
        else:
            try:
                res_txt = response.body.decode("UTF-8")
                logging.info("response body: "+ response.body.decode("UTF-8"))
                j_data = json.loads(res_txt)

                # (2) Crawl OperationPerformanceDaily
                var_list = [
                    'platId',
                    'ListingAmountDistribution_0_20W',
                    'ListingAmountDistribution_20_100W',
                    'ListingAmountDistribution_Above100W',
                    'ListingTypeDistribution_WebCredit',
                    'ListingTypeDistribution_SiteCredit',
                    'ListingTypeDistribution_BidTransfer',
                    'ListingTermDistribution_0_3M',
                    'ListingTermDistribution_3_6M',
                    'ListingTermDistribution_6_12M',
                    'ListingTermDistribution_Above12M',
                    'AmountValue',
                    'InflowValue',
                    'MoneyStockValue',
                    'PlatformReturn',
                    'AvgListingTerm',
                    'NumberOfBidders',
                    'AvgAmountOfBidders',
                    'NumberOfBiddersToBeRepaid',
                    'NumberOfBorrowers',
                    'AvgAmountOfBorrowers',
                    'NumberOfBids',
                    'NumberofBiddersToBePaid',
                    'AmountValue_RankingAbove',
                    'InflowValue_RankingAbove',
                    'MoneyStockValue_RankingAbove',
                    'PlatformReturn_RankingAbove',
                    'AvgListingTerm_RankingAbove',
                    'NumberOfBidders_RankingAbove',
                    'AvgAmountOfBidders_RankingAbove',
                    'NumberOfBiddersToBeRepaid_RankingAbove',
                    'NumberOfBorrowers_RankingAbove',
                    'AvgAmountOfBorrowers_RankingAbove',
                    'NumberOfBids_RankingAbove',
                    'NumberofBiddersToBePaid_RankingAbove',
                ]
                if 'basicValue' in j_data.keys():
                    platId = j_data['wdzjPlatId']
                    json_item = {}
                    json_item['platId'] = j_data['wdzjPlatId']
                    json_item['ListingAmountDistribution_0_20W'] = j_data['basicValue']['pie3'][0]['value']
                    json_item['ListingAmountDistribution_20_100W'] = j_data['basicValue']['pie3'][1]['value']
                    json_item['ListingAmountDistribution_Above100W'] = j_data['basicValue']['pie3'][2]['value']
                    json_item['ListingTypeDistribution_WebCredit'] = j_data['basicValue']['pie1'][0]['value']
                    json_item['ListingTypeDistribution_SiteCredit'] = j_data['basicValue']['pie1'][1]['value']
                    json_item['ListingTypeDistribution_BidTransfer'] = j_data['basicValue']['pie1'][2]['value']
                    json_item['ListingTermDistribution_0_3M'] = j_data['basicValue']['pie2'][0]['value']
                    json_item['ListingTermDistribution_3_6M'] = j_data['basicValue']['pie2'][1]['value']
                    json_item['ListingTermDistribution_6_12M'] = j_data['basicValue']['pie2'][2]['value']
                    json_item['ListingTermDistribution_Above12M'] = j_data['basicValue']['pie2'][3]['value']
                    json_item['AmountValue'] = j_data['phValue']['data']['y1'][0]
                    json_item['InflowValue'] = j_data['phValue']['data']['y1'][1]
                    json_item['MoneyStockValue'] = j_data['phValue']['data']['y1'][2]
                    json_item['PlatformReturn'] = j_data['phValue']['data']['y1'][3]
                    json_item['AvgListingTerm'] = j_data['phValue']['data']['y1'][4]
                    json_item['NumberOfBidders'] = j_data['phValue']['data']['y1'][5]
                    json_item['AvgAmountOfBidders'] = j_data['phValue']['data']['y1'][6]
                    json_item['NumberOfBiddersToBeRepaid'] = j_data['phValue']['data']['y1'][7]
                    json_item['NumberOfBorrowers'] = j_data['phValue']['data']['y1'][8]
                    json_item['AvgAmountOfBorrowers'] = j_data['phValue']['data']['y1'][9]
                    json_item['NumberOfBids'] = j_data['phValue']['data']['y1'][10]
                    json_item['NumberofBiddersToBePaid'] = j_data['phValue']['data']['y1'][11]
                    json_item['AmountValue_RankingAbove'] = j_data['phValue']['data']['y2'][0]
                    json_item['InflowValue_RankingAbove'] = j_data['phValue']['data']['y2'][1]
                    json_item['MoneyStockValue_RankingAbove'] = j_data['phValue']['data']['y2'][2]
                    json_item['PlatformReturn_RankingAbove'] = j_data['phValue']['data']['y2'][3]
                    json_item['AvgListingTerm_RankingAbove'] = j_data['phValue']['data']['y2'][4]
                    json_item['NumberOfBidders_RankingAbove'] = j_data['phValue']['data']['y2'][5]
                    json_item['AvgAmountOfBidders_RankingAbove'] = j_data['phValue']['data']['y2'][6]
                    json_item['NumberOfBiddersToBeRepaid_RankingAbove'] = j_data['phValue']['data']['y2'][7]
                    json_item['NumberOfBorrowers_RankingAbove'] = j_data['phValue']['data']['y2'][8]
                    json_item['AvgAmountOfBorrowers_RankingAbove'] = j_data['phValue']['data']['y2'][9]
                    json_item['NumberOfBids_RankingAbove'] = j_data['phValue']['data']['y2'][10]
                    json_item['NumberofBiddersToBePaid_RankingAbove'] = j_data['phValue']['data']['y2'][11]
                    item = load_item(json_item, OperationPerformanceDaily())
                    yield item
            except Exception as err:
                logging.info(err)

        # (1) Crawl DailyOperation_Part1
        if 'date' in j_data.keys():
            platId = j_data['wdzjPlatId']
            date_list = j_data['date']
            amountValue_list = j_data['amountValue']
            bidValue_list = j_data['bidValue']
            borValue_list = j_data['borValue']
            netFlowValue_list = j_data['netFlowValue']
            moneyStockValue_y1_list = j_data['moneyStockValue']['y1']
            moneyStockValue_y2_list = j_data['moneyStockValue']['y2']
            for (date, amountValue, bidValue, borValue, netFlowValue, moneyStockValue_y1, moneyStockValue_y2) in zip(
                    date_list,
                    amountValue_list,
                    bidValue_list,
                    borValue_list,
                    netFlowValue_list,
                    moneyStockValue_y1_list,
                    moneyStockValue_y2_list):
                item = DailyOperation_Part1()
                item.initValue()
                item['platId'] = platId
                item['date'] = date
                item['amountValue'] = amountValue
                item['bidValue'] = bidValue
                item['borValue'] = borValue
                item['netFlowValue'] = netFlowValue
                item['moneyStockValue_y1'] = moneyStockValue_y1
                item['moneyStockValue_y2'] = moneyStockValue_y2
                # item['CollectionTime'] = get_time()
                yield item


