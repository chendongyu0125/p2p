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
# from scrapy.utils.project import get_project_settings

class PlatformdetailsSpider(scrapy.Spider):
    name = 'PlatformDetails'
    # allowed_domains = ['wdzj.com']
    start_urls = ['https://www.wdzj.com/dangan/search?filter']


    def parse(self, response):


        baseURL="https://www.wdzj.com"
        filterURL="https://www.wdzj.com/dangan/search?filter"

        next_selector = response.xpath('//a[text()="下一页"]/@currentnum').extract()
        # current_page=response.xpath('//div[@class="pageList"]//a[@class="on"]/text()').extract()[0]
        for page in next_selector:
            next_page_url = filterURL + r"&currentPage=" + page
            yield Request(next_page_url)

        # get the snapshot of a platform


        xpathRoot='//ul[@class="terraceList"]/li[@class="item"]'
        selectors = response.xpath(xpathRoot)
        var_list = [
            'platNamePin',
            'platStatus',
            'platName',
            'platEarningsCode',
            'money_stock',
            'location',
            'onlineDate',
            'platReviewTag',
            'CreditRanking',
            'LabelList',
            'OverallScore',
            'NumberOfCommentators',

        ]
        xpath_leaf_list = [
            './/div[@class="itemTitle"]/h2/a/@href',
            './/div[@class="itemTag"]//i[contains(@class,"dangan_icon")]/../ul/li/text()',
            './/div[@class="itemTitle"]/h2/a/text()',
            './/div[contains(text(),"参考利率")]/label/em/text()',
            './/div[contains(text(),"待还余额")]/text()',
            './/div[contains(text(),"注册地")]/text()',
            './/div[contains(text(),"上线时间")]/text()',
            './/div[contains(text(),"网友印象")]/span/text()',
            './/em[contains(text(),"评级")]/strong/text()',
            './/div[@class="itemTitle"]/div[contains(@class,"itemTitleTag")]/em[not(contains(text(),"评级"))]/text()',
            './/div[contains(text(),"网友印象")]/strong/text()',
            './/div[contains(text(),"网友印象")]/em/text()',
        ]
        xpath_pair = dict(zip(var_list, xpath_leaf_list))
        for selector in selectors:
            item = PlatformSnapshot()
            item_loader = WDZJItemLoader(item=item, selector=selector)
            item_loader.add_xpath('location', xpath_pair['location'], MapCompose(lambda x: x.replace('注册地：', '')))
            item_loader.add_xpath('onlineDate', xpath_pair['onlineDate'], MapCompose(lambda x: x.replace('上线时间：', '')))
            for (var, xpath) in zip(var_list, xpath_leaf_list):
                item_loader.add_xpath(var, xpath, MapCompose(clean_values))
            item=item_loader.load_item()
            item['CollectionTime']=get_time()
            plat_url=baseURL+item['platNamePin']
            tries=0
            meta={"tries": tries}
            yield Request(plat_url, meta=meta, callback=self.parse_root)
            plat_gongshang_url=plat_url+"gongshang/"
            yield Request(plat_gongshang_url, meta=meta, callback=self.parse_gongshang)
            yield item


    def parse_root(self, response):
        """
        测试地址：https://www.wdzj.com/dangan/edsqb/

        :param response:
        :return:
        """
        tries = int(response.meta['tries'])
        if response.status == 555 and tries <100:
            tries+=1
            logging.debug("{0}访问过于频繁，疑似黑客攻击，休息一下。 tries = {1}".format(response.url, tries))
            time.sleep(int(self.settings.get('SLEEP_UNIT')) * tries)
            yield Request(response.url, meta={'tries': tries}, dont_filter=True, callback=self.parse_root)
        else:
            tries = 0
            meta = {"tries":tries}
            platId=clean_values(response.xpath('//input[@id="platId"]/@value').extract()[0])
            url = 'https://wwwservice.wdzj.com/api/plat/platData30Days?platId={0}'.format(platId)
            yield Request(url, meta=meta, callback=self.parse_data30day)
            url ='https://shuju.wdzj.com/plat-info-initialize.html'
            post_data = {"wdzjPlatId": platId}
            meta_platId = {'platId': platId, 'tries': 0}
            yield FormRequest(url, meta=meta_platId, formdata=post_data, callback=self.parse_initalize, dont_filter=True)
            url = 'https://www.wdzj.com/plat-center/platReview/getPlatReviewList?platId={0}&currentPage=1&pageSize=2&orderType=0'.format(platId)
            # yield Request(url, callback=self.parse_PlatformEvaluation_Part1)
            meta_platId = {'platId': platId, "tries": tries}
            yield Request(url, meta=meta_platId, callback=self.parse_PlatformEvaluation_Part1)
            url = 'https://www.wdzj.com/plat-center/platReview/getPlatReviewList?platId={0}&currentPage=1&pageSize=20&orderType=0'.format(platId)
            yield Request(url, meta=meta, callback=self.parse_PlatformReview)
            url='https://shuju.wdzj.com/wdzj-archives-chart.html?wdzjPlatId={0}&type=0&status=0'.format(platId)
            yield Request(url, meta=meta, callback=self.parse_DailyOperation_Part2)
            url = 'https://shuju.wdzj.com/plat-info-target.html'
            post_data = {"wdzjPlatId": str(platId), "type": "1", "target1": "7", "target2": "8"}
            yield FormRequest(url, formdata=post_data, meta=meta_platId, callback=self.parse_DailyOperation_Part3)
            post_data = {"wdzjPlatId": str(platId), "type": "1", "target1": "19", "target2": "20"}
            yield FormRequest(url, formdata=post_data, meta=meta_platId, callback=self.parse_DailyOperation_Part4)
            post_data = {"wdzjPlatId": str(platId), "type": "1", "target1": "21", "target2": "22"}
            yield FormRequest(url, formdata=post_data, meta=meta_platId, callback=self.parse_DailyOperation_Part5)
            url = 'https://www.wdzj.com/front/bbsInfo/{0}/1000000/1'.format(platId)  # 1000000-每页显示评论数，1-当前页数
            yield Request(url, meta=meta_platId, callback=self.parse_PostSnapshot)

            # (1) Crawl CompanyBackground_Part2
            varlist_CompanyBackground_Part2 = [
                'platId',
                'Fax',
                'ZipCode',
                'CompanyDescription',
                'ICPNumber',
                'Email',
                'CorrespondenceAddress',
                'CollectionTime',
            ]
            xpath_list_CompanyBackground_Part2 = [
                '//input[@id="platId"]/@value',
                '//em[contains(text(),"传真")]/../../div[2]/text()',
                '//em[contains(text(),"邮编")]/../../div[2]/text()',
                'string(//div[@class="da-gsjj"]/div)',
                '//em[text()="ICP号"]/../../div[2]/text()',
                '//em[contains(text(),"服务邮箱")]/../../div[2]/text()',
                '//em[contains(text(),"通信地址")]/../../div[2]/text()',
            ]
            item = CompanyBackground_Part2()
            item.initValue()
            bg_item_loader = WDZJItemLoader(item=item, response=response)
            for (var, xpath) in zip(varlist_CompanyBackground_Part2[:-1], xpath_list_CompanyBackground_Part2):
                bg_item_loader.add_xpath(var, xpath, MapCompose(clean_values))
            item = bg_item_loader.load_item()
            item['CollectionTime'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            yield item

            # (2) Crawl PlatformEvaluation_Part3
            varlist_PlatformEvaluation_Part3 = [
                'platId',
                'ReviewScore_Overall',
                'ReviewersNumber',
                'Comparision_Withdraw',
                'Comparison_Idle',
                'Comparison_Service',
                'Comparison_Experience',
                'OverallRanking',
                'OverallScore',
                'NumberOfWatchers',
            ]
            xpathlist_PlatformEvaluation_Part3 = [
                '//input[@id="platId"]/@value',
                '//div[@class="dianpBox"]//div[@class="dianpinbox"]/b/text()',
                '//div[@class="dianpBox"]//div[@class="dianpinbox"]/span/text()',
                'string(//dt[text()="与全部平台相比"]/../dd[1])',
                'string(//dt[text()="与全部平台相比"]/../dd[2])',
                'string(//dt[text()="与全部平台相比"]/../dd[3])',
                'string(//dt[text()="与全部平台相比"]/../dd[4])',
                '//div[@class="pt-info"]//div[text()="综合评级"]/../span/b/text()',
                '//div[@class="pt-info"]//div[text()="综合评级"]/../span[2]/text()',
                '//em[@id="followersBox"]/text()',
            ]
            item = PlatformEvaluation_Part3()
            item.initValue()
            pe3_item_loader = WDZJItemLoader(item=item, response=response)
            for (var, xpath) in zip(varlist_PlatformEvaluation_Part3, xpathlist_PlatformEvaluation_Part3):
                pe3_item_loader.add_xpath(var, xpath, MapCompose(clean_values))
            item = pe3_item_loader.load_item()
            item['CollectionTime'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
            yield item

            # (3) Crawl TopManager
            xpath_root_list = '//div[@class="da-ggjj"]//ul[@class="gglist"]/li'
            xpath_root_show = '//div[@class="da-ggjj"]//div[contains(@class, "ggshow")]'
            list_selectors = response.xpath(xpath_root_list)
            show_selectors = response.xpath(xpath_root_show)
            for (selector_list, selector_show) in zip(list_selectors, show_selectors):
                item = TopManager()
                list_item_loader = WDZJItemLoader(item=item, selector=selector_list)
                list_item_loader.add_xpath('Name', './a/span/text()', MapCompose(clean_values))
                list_item_loader.add_xpath('Position', './a/p/text()', MapCompose(clean_values))
                item = list_item_loader.load_item()
                show_item_loader = WDZJItemLoader(item=item, selector=selector_show)
                show_item_loader.add_xpath('Description','.//p[@class="cen"]/text()', MapCompose(clean_values))
                show_item_loader.add_xpath('ImageURL', './/img/@src', MapCompose(clean_values, lambda x: "http:"+x))
                item = show_item_loader.load_item()
                item['platId']=platId
                item['CollectionTime']=get_time()
                yield item
                #保存图片文件
                headers = {
                    'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36"
                }
                res = requests.get(item['ImageURL'], headers=headers)  # 将图片保存到本地
                if res.status_code==200:
                    save_image(item, res)


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
        tries = int(response.meta['tries'])
        if response.status == 555 and tries <100:
            tries+=1
            logging.debug("{0}访问过于频繁，疑似黑客攻击，休息一下。 tries = {1}".format(response.url, tries))
            time.sleep(int(self.settings.get('SLEEP_UNIT')) * tries)
            yield Request(response.url, meta={'tries': tries}, dont_filter=True, callback=self.parse_gongshang)
        else:
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

    def parse_data30day(self,response):
        """
        测试网页：
        https://wwwservice.wdzj.com/api/plat/platData30Days?platId=40
        https://wwwservice.wdzj.com/api/plat/platData30Days?platId=5216
        :param response:
        :return:
        """
        tries = int(response.meta['tries'])
        if response.status == 555 and tries < 100:
            tries += 1
            logging.debug("{0}访问过于频繁，疑似黑客攻击，休息一下。 tries = {1}".format(response.url, tries))
            time.sleep(int(self.settings.get('SLEEP_UNIT')) * tries)
            meta=response.meta
            meta['tries']=tries
            yield Request(response.url, meta=meta, dont_filter=True, callback=self.parse_data30day)
        else:

            j_data = json.loads(response.body.decode("UTF-8"))
            if j_data['data']['platOuterVo']=="":
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


    def parse_initalize(self, response):
        """
        (1) Crawl DailyOperation_Part1
        (2) Crawl OperationPerformanceDaily
        :param response:
        :return:
        """

        tries = int(response.meta['tries'])
        if response.status == 555 and tries < 100:
            tries += 1
            logging.debug("{0}访问过于频繁，疑似黑客攻击，休息一下。 tries = {1}".format(response.url, tries))
            time.sleep(int(self.settings.get('SLEEP_UNIT')) * tries)
            meta=response.meta
            meta['tries']=tries
            platId = response.meta['platId']
            post_data = {"wdzjPlatId": str(platId)}
            yield FormRequest(response.url, meta=meta, formdata=post_data, dont_filter=True, callback=self.parse_initalize)
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
                logging.error(err)

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

    def parse_PlatformEvaluation_Part1(self, response):
        """

        :param response:
        :return:
        """
        tries = int(response.meta['tries'])
        if response.status == 555 and tries < 100:
            tries += 1
            logging.debug("{0}访问过于频繁，疑似黑客攻击，休息一下。 tries = {1}".format(response.url, tries))
            time.sleep(int(self.settings.get('SLEEP_UNIT')) * tries)
            meta=response.meta
            meta['tries']=tries
            yield Request(response.url, meta=meta, dont_filter=True, callback=self.parse_PlatformEvaluation_Part1)
        else:
            j_data = json.loads(response.body)
            item = PlatformEvaluation_Part1()
            item['platId'] = response.meta['platId']
            item['good'] = j_data['data']['platReviewEvaluation']['good']
            item['bad'] = j_data['data']['platReviewEvaluation']['bad']
            item['midd'] = j_data['data']['platReviewEvaluation']['midd']
            yield item

    def parse_PlatformReview(self, response):
        """

        :param response:
        :return:
        """
        tries = int(response.meta['tries'])
        if response.status == 555 and tries < 100:
            tries += 1
            logging.debug("{0}访问过于频繁，疑似黑客攻击，休息一下。 tries = {1}".format(response.url, tries))
            time.sleep(int(self.settings.get('SLEEP_UNIT')) * tries)
            meta = response.meta
            meta['tries'] = tries
            yield Request(response.url, meta=meta, dont_filter=True, callback=self.parse_PlatformReview)
        else:
            res_txt = response.body.decode("UTF-8")
            logging.info(response.body.decode("UTF-8"))
            j_data = json.loads(res_txt)
            if j_data['message'] == '访问次数过于频繁' and int(response.meta['tries']) < 100:
                tries = int(response.meta['tries']) + 1
                logging.info("访问次数过于频繁，休息一下. tries= {0}".format(tries))
                time.sleep(int(self.settings.get('SLEEP_UNIT')) * tries)
                tries = tries + 1
                yield Request(response.url, meta={'tries': tries}, dont_filter=True, callback=self.parse_PlatformReview)
            else:
                try:
                    platId = j_data['data']['platReviewSearchVo']['platId']
                    total_page = int(j_data['data']['pagination']['totalPage'])
                    # print(total_page)
                    current_page = int(j_data['data']['pagination']['currentPage'])
                    # print(current_page)
                    if current_page < total_page:
                        next_url = 'https://www.wdzj.com/plat-center/platReview/getPlatReviewList?platId={0}&currentPage={1}&pageSize=20&orderType=0'.format(
                            platId, current_page + 1)
                        # print(next_url)
                        yield Request(next_url, meta={'tries': 0}, dont_filter=True, callback=self.parse_PlatformReview)

                    review_list = j_data['data']['pagination']['list']
                    for review in review_list:
                        if 'replyList' in review.keys():
                            for reply in review['replyList']:
                                item = load_item(reply, PlatformReviewReply())
                                yield item
                            review.pop('replyList')
                            review['hasReply'] = 1
                        item = load_item(review, PlatformReview())
                        yield item
                except Exception as err:
                    logging.info(err)

    def parse_DailyOperation_Part2(self, response):
        """

        :param response:
        :return:
        """
        tries = int(response.meta['tries'])
        if response.status == 555 and tries < 100:
            tries += 1
            logging.debug("{0}访问过于频繁，疑似黑客攻击，休息一下。 tries = {1}".format(response.url, tries))
            time.sleep(int(self.settings.get('SLEEP_UNIT')) * tries)
            meta = response.meta
            meta['tries'] = tries
            yield Request(response.url, meta=meta, dont_filter=True, callback=self.parse_DailyOperation_Part2)
        else:
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

    def parse_DailyOperation_Part3(self, response):
        """

        :param response:
        :return:
        """
        tries = int(response.meta['tries'])
        if response.status == 555 and tries < 100:
            tries += 1
            logging.debug("{0}访问过于频繁，疑似黑客攻击，休息一下。 tries = {1}".format(response.url, tries))
            time.sleep(int(self.settings.get('SLEEP_UNIT')) * tries)
            meta = response.meta
            meta['tries'] = tries
            platId=response.meta['platId']
            post_data = {"wdzjPlatId": platId, "type": "1", "target1": "7", "target2": "8"}
            # yield Request(response.url, meta=meta, dont_filter=True, callback=self.parse_DailyOperation_Part3)
            yield FormRequest(response.url, formdata=post_data, meta=meta, dont_filter=True,callback=self.parse_DailyOperation_Part3)
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

    def parse_DailyOperation_Part4(self, response):
        """

        :param response:
        :return:
        """
        tries = int(response.meta['tries'])
        if response.status == 555 and tries < 100:
            tries += 1
            logging.debug("{0}访问过于频繁，疑似黑客攻击，休息一下。 tries = {1}".format(response.url, tries))
            time.sleep(int(self.settings.get('SLEEP_UNIT')) * tries)
            meta = response.meta
            meta['tries'] = tries
            platId = response.meta['platId']
            post_data = {"wdzjPlatId": platId, "type": "1", "target1": "19", "target2": "20"}
            yield FormRequest(response.url, formdata= post_data, meta=meta, dont_filter=True, callback=self.parse_DailyOperation_Part4)
        else:
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

    def parse_DailyOperation_Part5(self, response):
        tries = int(response.meta['tries'])
        if response.status == 555 and tries < 100:
            tries += 1
            logging.debug("{0}访问过于频繁，疑似黑客攻击，休息一下。 tries = {1}".format(response.url, tries))
            time.sleep(int(self.settings.get('SLEEP_UNIT')) * tries)
            meta = response.meta
            meta['tries'] = tries
            platId = response.meta['platId']
            post_data = {"wdzjPlatId": platId, "type": "1", "target1": "21", "target2": "22"}
            yield FormRequest(response.url, formdata=post_data, meta=meta, dont_filter=True, callback=self.parse_DailyOperation_Part5)
        else:
            j_data = json.loads(response.body)
            platId = response.meta['platId']
            date_list = j_data['date']
            d1_list = j_data['data1']
            d2_list = j_data['data2']
            for (d, d1, d2) in zip(date_list, d1_list, d2_list):
                item = DailyOperation_Part5()
                item.initValue()
                item['platId'] = platId
                item['date'] = d
                item['newBidderAmount'] = d1
                item['oldBidderAmount'] = d2
                item['CollectionTime'] = get_time()
                yield item



    def parse_PostSnapshot(self, response):
        """
        test instance:
        scrapy parse --callback=parse_PostSnapshot "https://www.wdzj.com/front/bbsInfo/40/1000000/1"

        :param response:
        :return:
        """
        tries = int(response.meta['tries'])
        if response.status == 555 and tries < 100:
            tries += 1
            logging.debug("{0}访问过于频繁，疑似黑客攻击，休息一下。 tries = {1}".format(response.url, tries))
            time.sleep(int(self.settings.get('SLEEP_UNIT')) * tries)
            meta = response.meta
            meta['tries'] = tries
            yield Request(response.url, meta=meta, dont_filter=True, callback=self.parse_PostSnapshot)
        else:
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
                meta_tid = {"tid": tid, "tries": 0}
                url = 'https://bbs.wdzj.com/thread-{0}-1-1.html'.format(tid)
                yield Request(url, meta=meta_tid, callback=self.parse_PostDetail)
                url = 'https://bbs.wdzj.com/thread/getAuthorComment?type=1&author=&tid={0}&page=1&page_size=50'.format(tid)
                meta_pg = {"tid": tid, "tries": 0, "current_page": 1}
                yield Request(url, meta=meta_pg, callback=self.parse_PostReply)
                yield item

    def parse_PostDetail(self, response):
        tries = int(response.meta['tries'])
        if response.status == 555 and tries < 100:
            tries += 1
            logging.debug("{0}访问过于频繁，疑似黑客攻击，休息一下。 tries = {1}".format(response.url, tries))
            time.sleep(int(self.settings.get('SLEEP_UNIT')) * tries)
            meta = response.meta
            meta['tries'] = tries
            yield Request(response.url, meta=meta, dont_filter=True, callback=self.parse_PostDetail)
        else:
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
            msg = item['Content']
            item['Content'] = clean_values(msg)
            yield item


    def parse_PostReply(self, response):
        tries = int(response.meta['tries'])
        if response.status == 555 and tries < 100:
            tries += 1
            logging.debug("{0}访问过于频繁，疑似黑客攻击，休息一下。 tries = {1}".format(response.url, tries))
            time.sleep(int(self.settings.get('SLEEP_UNIT')) * tries)
            meta = response.meta
            meta['tries'] = tries
            yield Request(response.url, meta=meta, dont_filter=True, callback=self.parse_PostReply)
        else:
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
                meta = {"tid": tid, "current_page": next_page, "tries": 0}
                yield Request(url, meta=meta, callback=self.parse_PostReply)

            # 抓取当前页面内容
            comments = j_data['comment']
            for comment in comments:
                item = PostReply()
                item.initValue()
                item_loader = WDZJItemLoader(item=item, response=response)
                for var in varlist[:-1]:
                    item_loader.add_value(var, comment[var], MapCompose(clean_values, remove_slash))
                item = item_loader.load_item()
                txt = item['message']
                item['message'] = remove_tags(txt)
                yield item












