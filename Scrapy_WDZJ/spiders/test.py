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


class testSpider(scrapy.Spider):
    name = 'test'
    allowed_domains = ['wdzj.com']
    start_urls = ['https://www.wdzj.com/dangan/search?filter']
    # headers={
    # 'User-Agent': "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0.3396.99 Safari/537.36"
    #      }

    def parse(self, response):
        baseURL="https://www.wdzj.com"
        filterURL="https://www.wdzj.com/dangan/search?filter"

        # get the snapshot of a platform
        var_list=[
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
        xpath_leaf_list=[
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

        xpath_pair=dict(zip(var_list, xpath_leaf_list))

        xpathRoot='//ul[@class="terraceList"]/li[@class="item"]'

        selectors = response.xpath(xpathRoot)

        for selector in selectors:
            item = PlatformSnapshot()
            item.initValue()
            item_loader = WDZJItemLoader(item=item, selector=selector)
            for (var, xpath) in zip(var_list, xpath_leaf_list):
                item_loader.add_xpath(var, xpath, MapCompose(clean_values))
            item_loader.add_xpath('location', xpath_pair['location'], MapCompose(lambda x: x.replace('注册地：', '')))
            item_loader.add_xpath('onlineDate', xpath_pair['onlineDate'], MapCompose(lambda x: x.replace('上线时间：', '')))
            item=item_loader.load_item()
            item['CollectionTime']=get_time()
            yield item


        # get the next index URL list and yield Requests
        next_selector = response.xpath('//a[text()="下一页"]/@currentnum').extract()
        # current_page=response.xpath('//div[@class="pageList"]//a[@class="on"]/text()').extract()[0]
        for page in next_selector:
            next_page_url = filterURL + r"&currentPage=" + page
            yield Request(next_page_url)


        # go into details of a platform
        # 抓取网贷平台更详细的信息

        sub_urls = response.xpath('//div[@class="itemTitle"]/h2/a/@href').extract()  # "/dangan/nwd/"
        urls = [baseURL+sub_url for sub_url in sub_urls]
        for url in urls:

            yield Request(url, callback=self.parse_root)





    def parse_root(self, response):
        """

        :param response:
        :return:
        """


        ## （1）抓取高管数据
        platId=clean_values(response.xpath('//input[@id="platId"]/@value').extract()[0])




        # 通过platId抓取更多数据
        # (1) 平台详细信息： PlatformDetail
        # call_back=self.parse_PlatformDetail
        # url = https://wwwservice.wdzj.com/api/plat/platData30Days?platId=40
        url = 'https://wwwservice.wdzj.com/api/plat/platData30Days?platId={0}'.format(platId)
        yield Request(url, callback=self.parse_PlatformDetail)

    def parse_CompanyBackground_Part1(self, response):
        """
        处理提取背景信息
        测试代码：scrapy parse --callback=parse_CompanyBackground_Part1
            "https://www.wdzj.com/dangan/hxd5/gongshang/"
            "https://www.wdzj.com/dangan/hzed/gongshang/" （含公司变更信息）
            "https://www.wdzj.com/dangan/edsqb/gongshang/"
        :param response:
        :return:
        """
        xpath_list=[
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

        var_list=[
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

        item=CompanyBackground_Part1()
        item.initValue()
        item_loader=WDZJItemLoader(item=item, response=response)
        for (var, xpath) in zip(var_list, xpath_list):
            item_loader.add_xpath(var, xpath, MapCompose(clean_values))
        item=item_loader.load_item()
        platId = item['platId']
        # item['CollectionTime']=str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
        yield item



        # 以下是其他信息
        # （2）CompanyChangeHistory （excel工作簿：工商登记信息， 工商变更信息）
        # （3）AbnormalOperationHistory （excel工作簿：工商登记信息， 异常经营信息）
        # （4）CapitalStructure （excel工作簿：工商登记信息， 股权信息）

        # （2）公司变更信息
        var_list = [

            'ChangeDate',
            'ChangeType',
            'InfoBeforeChange',
            'InfoAfterChange',
        ]


        xpath_root = '//div[text()="变更记录"]/..//table[@class="table-ic"]/tbody[@class="tbody"]/tr'

        xpath_leaf_list = [

            './td[1]/text()',
            './td[2]/text()',
            './td[3]/text()',
            './td[4]/text()',
        ]

        selectors = response.xpath(xpath_root)
        # self.log(selectors)
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

        # （3）AbnormalOperationHistory （excel工作簿：工商登记信息， 异常经营信息）
        # 例如："https://www.wdzj.com/dangan/wlb/gongshang/"
        var_list = [

            'SerialNumber',
            'InListReason',
            'InlistDate',
            'InListInstitution',
            'RemoveListReason',
            'RemoveListDate',
            'RemoveListInstitution',

        ]

        xpath_root = '//div[text()="异常经营"]/..//table[@class="table-ic"]/tbody[@class="tbody"]/tr'

        xpath_leaf_list = [

            './td[1]/text()',
            './td[2]/text()',
            './td[3]/text()',
            './td[4]/text()',
            './td[5]/text()',
            './td[6]/text()',
            './td[7]/text()',
        ]

        selectors = response.xpath(xpath_root)
        for selector in selectors:
            item = AbnormalOperationHistory()
            item.initValue()
            item['platId'] = platId
            item_loader = WDZJItemLoader(item=item, selector=selector)
            for (var, xpath) in zip(var_list, xpath_leaf_list):
                item_loader.add_xpath(var, xpath, MapCompose(clean_values))
                # item[var]=clean_values(selector.xpath(xpath).extract())
            item = item_loader.load_item()
            item['CollectionTime'] = get_time()
            yield item


        # (4)股权信息
        xpath_root = '//div[@id="gqInfoBox"]//table[@class="table-ic"]/tbody[@class="tbody"]/tr'

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


        selectors = response.xpath(xpath_root)
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





    def parse_CompanyChangeHistory(self, response):
        """
        工商变更信息
        :param response:
        :return:
        """
        var_list = [

            'ChangeDate',
            'ChangeType',
            'InfoBeforeChange',
            'InfoAfterChange',
        ]

        xpath_root = '//div[text()="变更记录"]/..//table[@class="table-ic"]/tbody[@class="tbody"]/tr'

        xpath_leaf_list = [

            './td[1]/text()',
            './td[2]/text()',
            './td[3]/text()',
            './td[4]/text()',
        ]
        platId = clean_values(response.xpath('//input[@id="platId"]/@value').extract())
        selectors=response.xpath(xpath_root)
        for selector in selectors:
            item=CompanyChangeHistory()
            item.initValue()
            item_loader=WDZJItemLoader(item=item, selector=selector)
            for (var, xpath) in zip(var_list, xpath_leaf_list):
                item_loader.add_xpath(var, xpath, MapCompose(clean_values))
            item = item_loader.load_item()
            item['platId'] = platId
            item['CollectionTime'] = get_time()
            yield item

    def parse_AbnormalOperationHistory(self, response):

        # 例如："https://www.wdzj.com/dangan/wlb/gongshang/"
        var_list = [

            'SerialNumber',
            'InListReason',
            'InlistDate',
            'InListInstitution',
            'RemoveListReason',
            'RemoveListDate',
            'RemoveListInstitution',

        ]

        xpath_root='//div[text()="异常经营"]/..//table[@class="table-ic"]/tbody[@class="tbody"]/tr'

        xpath_leaf_list = [

            './td[1]/text()',
            './td[2]/text()',
            './td[3]/text()',
            './td[4]/text()',
            './td[5]/text()',
            './td[6]/text()',
            './td[7]/text()',
        ]
        platId = clean_values(response.xpath('//input[@id="platId"]/@value').extract())
        selectors = response.xpath(xpath_root)
        for selector in selectors:
            item = AbnormalOperationHistory()
            item.initValue()
            item['platId']=platId
            item_loader = WDZJItemLoader(item=item, selector=selector)
            for (var, xpath) in zip(var_list, xpath_leaf_list):
                item_loader.add_xpath(var, xpath, MapCompose(clean_values))
                # item[var]=clean_values(selector.xpath(xpath).extract())
            item = item_loader.load_item()
            item['CollectionTime'] = get_time()
            yield item

    def parse_CapitalStructure(self, response):

        xpath_root = '//div[@id="gqInfoBox"]//table[@class="table-ic"]/tbody[@class="tbody"]/tr'

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
        platId = clean_values(response.xpath('//input[@id="platId"]/@value').extract()[0])

        selectors = response.xpath(xpath_root)
        for selector in selectors:
            item = CapitalStructure()
            item.initValue()
            item_loader= WDZJItemLoader(item=item, selector=selector)
            for (var, xpath) in zip(var_list, xpath_leaf_list):
                item_loader.add_xpath(var, xpath)
            item = item_loader.load_item()
            item['platId'] = platId
            item['CollectionTime'] = get_time()
            yield item

    def parse_gongshang(self, response):
        varlist_CompanyBackground_Part1 = {
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
            'CollectionTime',
        }

        varlist_CompanyChangeHistory = {
            'platId',
            'ChangeDate',
            'ChangeType',
            'InfoBeforeChange',
            'InfoAfterChange',
            'CollectionTime',
        }

        varlist_AbnormalOperationHistory = {
            'platId',
            'SerialNumber',
            'InListReason',
            'InlistDate',
            'InListInstitution',
            'RemoveListReason',
            'RemoveListDate',
            'RemoveListInstitution',
            'CollectionTime',
        }

        varlist_CapitalStructure = {
            'platId',
            'ShareholderName',
            'SharePercent',
            'SubscribedCapitalContribution',
            'CollectionTime',
        }

    def parse_CoreOperationPerformanceDaily(self, response):
        """
        抓取昨日的核心运营数据

        :param response:
        :return:
        """

        # print(response.body)

        item = CoreOperationPerformanceDaily()
        item.initValue()
        item['CollectionTime'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        j_data=json.loads(response.body.decode("UTF-8"))
        item['platId'] = j_data['wdzjPlatId']
        item['ListingAmountDistribution_0_20W'] = j_data['basicValue']['pie3'][0]['value']
        item['ListingAmountDistribution_20_100W'] = j_data['basicValue']['pie3'][1]['value']
        item['ListingAmountDistribution_Above100W'] = j_data['basicValue']['pie3'][2]['value']
        item['ListingTypeDistribution_WebCredit'] = j_data['basicValue']['pie1'][0]['value']
        item['ListingTypeDistribution_SiteCredit'] = j_data['basicValue']['pie1'][1]['value']
        item['ListingTypeDistribution_BidTransfer'] = j_data['basicValue']['pie1'][2]['value']
        item['ListingTermDistribution_0_3M'] = j_data['basicValue']['pie2'][0]['value']
        item['ListingTermDistribution_3_6M'] = j_data['basicValue']['pie2'][1]['value']
        item['ListingTermDistribution_6_12M'] = j_data['basicValue']['pie2'][2]['value']
        item['ListingTermDistribution_Above12M'] = j_data['basicValue']['pie2'][3]['value']
        item['AmountValue'] = j_data['phValue']['data']['y1'][0]
        item['InflowValue'] = j_data['phValue']['data']['y1'][1]
        item['MoneyStockValue'] = j_data['phValue']['data']['y1'][2]
        item['PlatformReturn'] = j_data['phValue']['data']['y1'][3]
        item['AvgListingTerm'] = j_data['phValue']['data']['y1'][4]
        item['NumberOfBidders'] = j_data['phValue']['data']['y1'][5]
        item['AvgAmountOfBidders'] = j_data['phValue']['data']['y1'][6]
        item['NumberOfBiddersToBeRepaid'] = j_data['phValue']['data']['y1'][7]
        item['NumberOfBorrowers'] = j_data['phValue']['data']['y1'][8]
        item['AvgAmountOfBorrowers'] = j_data['phValue']['data']['y1'][9]
        item['NumberOfBids'] = j_data['phValue']['data']['y1'][10]
        item['NumberofBiddersToBePaid'] = j_data['phValue']['data']['y1'][11]
        item['AmountValue_RankingAbove'] = j_data['phValue']['data']['y2'][0]
        item['InflowValue_RankingAbove'] = j_data['phValue']['data']['y2'][1]
        item['MoneyStockValue_RankingAbove'] = j_data['phValue']['data']['y2'][2]
        item['PlatformReturn_RankingAbove'] = j_data['phValue']['data']['y2'][3]
        item['AvgListingTerm_RankingAbove'] = j_data['phValue']['data']['y2'][4]
        item['NumberOfBidders_RankingAbove'] = j_data['phValue']['data']['y2'][5]
        item['AvgAmountOfBidders_RankingAbove'] = j_data['phValue']['data']['y2'][6]
        item['NumberOfBiddersToBeRepaid_RankingAbove'] = j_data['phValue']['data']['y2'][7]
        item['NumberOfBorrowers_RankingAbove'] = j_data['phValue']['data']['y2'][8]
        item['AvgAmountOfBorrowers_RankingAbove'] = j_data['phValue']['data']['y2'][9]
        item['NumberOfBids_RankingAbove'] = j_data['phValue']['data']['y2'][10]
        item['NumberofBiddersToBePaid_RankingAbove'] = j_data['phValue']['data']['y2'][11]
        yield item


    def parse_OperationPerformanceMonthly(self, response):
        """
        抓取最近30天的运营数据

        :param response:
        :return:
        """
        item = OperationPerformanceMonthly()
        item.initValue()
        item['CollectionTime'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        j_data = json.loads(response.body.decode("UTF-8"))

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
        yield item

    def parse_PlatformDetail(self, response):
        """
        测试网页 scrapy parse --callback=parse_PlatformDetail 'https://wwwservice.wdzj.com/api/plat/platData30Days?platId=5216'
        :param response:
        :return:
        """
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

        j_data = json.loads(response.body)
        item = PlatformDetail()
        item.initValue()
        item_loader = WDZJItemLoader(item=item, response=response)
        for var in var_list:
            item_loader.add_value(var, j_data['data']['platOuterVo'][var], MapCompose(clean_values))
        item = item_loader.load_item()
        item['CollectionTime']=get_time()
        yield item

        """
                抓取最近30天的运营数据
    
                :param response:
                :return:
                """
        if 'platShujuMap' not in j_data['data'].keys():
            return
        item = OperationPerformanceMonthly()
        item.initValue()
        item['CollectionTime'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        #j_data = json.loads(response.body.decode("UTF-8"))

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
        yield item


    def parse_PlatformEvaluationMonthly_Recommend(self, response):
        """
        网址：https://shuju.wdzj.com/plat-info-initialize.html
        抓取评价数据，推荐/不推荐
        :param response:
        :return:
        """
        item = PlatformEvaluationMonthly_Recommend()
        item.initValue()
        item['CollectionTime'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        j_data = json.loads(response.body.decode("UTF-8"))

        item['platId'] = j_data['data']['platReviewEvaluation']['platId']
        item['good'] = j_data['data']['platReviewEvaluation']['good']
        item['midd'] = j_data['data']['platReviewEvaluation']['midd']
        item['bad'] = j_data['data']['platReviewEvaluation']['bad']
        yield item


    def parse_PlatformEvaluationMonthly_DetailScore(self, response):
        """
        抓取各维度评价
        :param response:
        :return:
        """
        item = PlatformEvaluationMonthly_DetailScore
        item.initValue()
        item['CollectionTime'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        j_data = json.loads(response.body.decode("UTF-8"))

        item['platId'] = j_data['data']['platOuterVo']['platId']
        item['drawScore'] = j_data['data']['platOuterVo']['drawScore']
        item['delayScore'] = j_data['data']['platOuterVo']['delayScore']
        item['serviceScore'] = j_data['data']['platOuterVo']['serviceScore']
        item['experienceScore'] = j_data['data']['platOuterVo']['experienceScore']
        item['drawScoreDetail'] = j_data['data']['platOuterVo']['drawScoreDetail']
        item['delayScoreDetail'] = j_data['data']['platOuterVo']['delayScoreDetail']
        item['serviceScoreDetail'] = j_data['data']['platOuterVo']['serviceScoreDetail']
        item['experienceScoreDetail'] = j_data['data']['platOuterVo']['experienceScoreDetail']
        yield item




    def parse_PlatformEvaluationMonthly_GeneralScore(self, response):
        """
        抓取总体评价信息
        :param response:
        :return:
        """
        item = PlatformEvaluationMonthly_GeneralScore
        item.initValue()
        item['CollectionTime'] = str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def parse_OperationPerformanceDaily(self, response):
        # testing sample:
        # scrapy parse --callback=parse_OperationPerformanceDaily "
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

        j_data=json.loads(response.body)

        json_item={}
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

        item = OperationPerformanceDaily()
        item.initValue()

        item_loader = WDZJItemLoader(item, response=response)
        for var in var_list:
            item_loader(var, json_item[var], MapCompose(clean_values))
        item=item_loader.load_item()
        item['CollectionTime'] = get_time()
        yield item


        # 抓取DailyOperation_Part1

        var_list = [
            'platId',
            'date',
            'amountValue',
            'bidValue',
            'borValue',
            'netFlowValue',
            'moneyStockValue_y1',
            'moneyStockValue_y2',
            'CollectionTime',
        ]

        j_data = json.loads(response.body)

        platId = j_data['wdzjPlatId']
        date_list = j_data['phValue']['data']['date']
        amountValue_list = j_data['phValue']['data']['amountValue']
        bidValue_list = j_data['phValue']['data']['bidValue']
        borValue_list = j_data['phValue']['data']['borValue']
        netFlowValue_list = j_data['phValue']['data']['netFlowValue']
        moneyStockValue_y1_list = j_data['phValue']['data']['moneyStockValue']['y1']
        moneyStockValue_y2_list = j_data['phValue']['data']['moneyStockValue']['y2']

        for (date, amountValue, bidValue, borValue, netFlowValue, moneyStockValue_y1, moneyStockValue_y2) in zip(date_list,
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
            item['CollectionTime'] = get_time()
            yield item

    def parse_OperationPerformanceMonthly(self, response):
        if response.status_code!=200:
            return

        var_list = [
            'platId',
            'amount',
            'bidder_num',
            'borrower_num',
            'bor_top10',
            'bid_top10',
            'net_inflow',
            'amount_percent',
            'bidder_num_percent',
            'borrower_num_percent',
            'bor_top10_change',
            'bid_top10_change',
            'net_inflow_percent',
            'CollectionTime',
        ]



        j_data=json.loads(response.body)

        json_item={}
        json_item['platId'] = j_data['data']['platOuterVo']['platId']
        json_item['amount'] = j_data['data']['platShujuMap']['amount']
        json_item['bidder_num'] = j_data['data']['platShujuMap']['bidder_num']
        json_item['borrower_num'] = j_data['data']['platShujuMap']['borrower_num']
        json_item['bor_top10'] = j_data['data']['platShujuMap']['bor_top10']
        json_item['bid_top10'] = j_data['data']['platShujuMap']['bid_top10']
        json_item['net_inflow'] = j_data['data']['platShujuMap']['net_inflow']
        json_item['amount_percent'] = j_data['data']['platShujuMap']['amount_percent']
        json_item['bidder_num_percent'] = j_data['data']['platShujuMap']['bidder_num_percent']
        json_item['borrower_num_percent'] = j_data['data']['platShujuMap']['borrower_num_percent']
        json_item['bor_top10_change'] = j_data['data']['platShujuMap']['bor_top10_change']
        json_item['bid_top10_change'] = j_data['data']['platShujuMap']['bid_top10_change']
        json_item['net_inflow_percent'] = j_data['data']['platShujuMap']['net_inflow_percent']

        item = OperationPerformanceMonthly()
        item.initValue()
        item_loader = WDZJItemLoader(item, response=response)
        for var in var_list:
            item_loader.add_value(var, json_item[var], MapCompose(clean_values))
        item = item_loader.load_item()
        item['CollectionTime']=get_time()
        yield item



    def parse_DailyOperation_Part1(self, response):

        var_list = [
            'platId',
            'date',
            'amountValue',
            'bidValue',
            'borValue',
            'netFlowValue',
            'moneyStockValue_y1',
            'moneyStockValue_y2',
            'CollectionTime',
        ]


        j_data=json.loads(response.body)

        platId=j_data['wdzjPlatId']
        date_list=j_data['phValue']['data']['date']
        amountValue_list=j_data['phValue']['data']['amountValue']
        bidValue_list = j_data['phValue']['data']['bidValue']
        borValue_list = j_data['phValue']['data']['borValue']
        netFlowValue_list = j_data['phValue']['data']['netFlowValue']
        moneyStockValue_y1_list = j_data['phValue']['data']['moneyStockValue']['y1']
        moneyStockValue_y2_list = j_data['phValue']['data']['moneyStockValue']['y2']

        for (date, amountValue, bidValue, borValue, netFlowValue, moneyStockValue_y1, moneyStockValue_y2) in zip(date_list, amountValue_list, bidValue_list, borValue_list, netFlowValue_list, moneyStockValue_y1_list, moneyStockValue_y2_list):
            item = DailyOperation_Part1()
            item.initValue()
            item['platId']=platId
            item['date']=date
            item['amountValue']=amountValue
            item['bidValue']=bidValue
            item['borValue']=borValue
            item['netFlowValue']=netFlowValue
            item['moneyStockValue_y1']=moneyStockValue_y1
            item['moneyStockValue_y2']=moneyStockValue_y2
            item['CollectionTime']=get_time()
            yield item


    def parse_DailyOperation_Part2(self, response):
        var_list = [
            'platId',
            'date',
            'platformReturn',
            'amountValue',
            'CollectionTime',
        ]

        j_data = json.loads(response.body)
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
        var_list = [
            'platId',
            'date',
            'bidValuePerCapita',
            'borValuePerCapita',
            'CollectionTime',
        ]

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
        varlist = [
            'platId',
            'date',
            'newBidders',
            'oldBidders',
            'CollectionTime',
        ]


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
        var_list =[
            'platId',
            'date',
            'newBidderAmount',
            'oldBidderAmount',
            'CollectionTime'
        ]
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


    def parse_PlatformReview(self, response):
        varlist = [
            'platId',
            'platName',
            'platPin',
            'reviewId',
            'reviewUserName',
            'reviewUserId',
            'reviewDate',
            'reviewEvaluation',
            'salaryguard',
            'withdrawSpeed',
            'websiteExperience',
            'serviceAttitude',
            'tagList',
            'reviewContent',
            'useful',
            'noUseful',
            'parentReviewPlatFlag',
            'orderType',
            'reviewPlatFlag',
            'CollectionTime',
        ]


        j_data = json.loads(response.body)
        platId = j_data['data']['platReviewSearchVo']['platId']

        total_page = int(j_data['data']['pagination']['totalPage'])
        current_page = int(j_data['data']['pagination']['currentPage'])
        if current_page < total_page:
            next_url = 'https://www.wdzj.com/plat-center/platReview/getPlatReviewList?platId={0}&currentPage={1}&pageSize=20&orderType=0'.format(platId, current_page+1)
            yield Request(next_url)

        review_list = j_data['data']['pagination']['list']

        for review in review_list:
            item = PlatformReview()
            item.initValue()
            for var in varlist[:-1]:
                item[var]=review[var]
            item['CollectionTime']=get_time()
            yield item





    def parse_PlatformEvaluation_Part1(self, response):
        varlist = [
            'platId',
            'good',
            'midd',
            'bad',
            'CollectionTime',
        ]

        j_data = json.loads(response.body)
        item = PlatformEvaluation_Part1()
        item.initValue()
        item['platId']=j_data['data']['platReviewEvaluation']['platId']
        item['good']= j_data['data']['platReviewEvaluation']['good']
        item['bad'] = j_data['data']['platReviewEvaluation']['bad']
        item['midd'] = j_data['data']['platReviewEvaluation']['midd']
        item['CollectionTime'] = get_time()
        yield item


    def parse_PlatformEvaluation_Part2(self, response):
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
        j_data = json.loads(response.body)
        item = PlatformEvaluation_Part2()
        item.initValue()
        for var in varlist[:-1]:
            item[var]=j_data['data']['platOuterVo'][var]
        item['CollectionTime'] = get_time()
        yield item

    def parse_PlatformEvaluation_Part3(self, response):
        var_list = [
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
            'CollectionTime',
        ]

        xpath_list = [
            '//input[@id="platId"]/@value',
            '//div[@class="dianpBox"]//div[@class="dianpinbox"]/b/text()',
            '//div[@class="dianpBox"]//div[@class="dianpinbox"]/span/text()',
            'string(//dt[text()="与全部平台相比"]/../dd[1])',
            'string(//dt[text()="与全部平台相比"]/../dd[2])',
            'string(//dt[text()="与全部平台相比"]/../dd[3])',
            'string(//dt[text()="与全部平台相比"]/../dd[4])',
            '//div[@class="pt-info"]//div[text()="综合评级"]/../span/b/text()',
            '//div[@class="pt-info"]//div[text()="综合评级"]/../span[2]/text()',
            '//span[contains(text()[1], "投友已关注")]/../span/em/text()',
        ]

        item = PlatformEvaluation_Part3()
        item.initValue()
        item_loader = WDZJItemLoader(item, response=response)
        for (var, xpath) in zip(var_list, xpath_list):
            item_loader.add_xpath(var, xpath, MapCompose(clean_values))
        item = item_loader.load_item()
        item['CollectionTime']= get_time()
        yield item



    def parse_PostSnapshot(self, response):
        # test instance:
        # scrapy parse --callback=parse_PostSnapshot "https://www.wdzj.com/front/bbsInfo/40/1000000/1"
        varlist = [

            'AuthorID',
            'tid',
            'author_name',
            'terminal',
            'PostingDate',
            'PostAbstract',
            'PostTitle',
        ]



        xpath_root ="//ul/li"

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



        platId = parse_digits(response.url)

        # platId = response.meta['platId']

        selectors = response.xpath(xpath_root)
        for selector in selectors:
            item = PostSnapshot()
            item.initValue()
            item['platId']=platId
            item_loader = WDZJItemLoader(item=item, selector=selector)
            item_loader.add_xpath('tid', xpath_pair['tid'], MapCompose(parse_digits))
            item_loader.add_xpath('AuthorID', xpath_pair['AuthorID'], MapCompose(parse_digits))
            for (var, xpath) in zip(varlist, xpath_leaf_list):
                item_loader.add_xpath(var, xpath, MapCompose(clean_values))

            item = item_loader.load_item()

            item['CollectionTime'] = get_time()
            yield item

            tid = item['tid']

            # (1) 抓取帖子内容
            url = 'https://bbs.wdzj.com/thread-{0}-1-1.html'.format(tid)
            yield Request(url, meta={"tid": tid}, callback=self.parse_PostDetail)

            # （2）抓取帖子回复信息
            url = 'https://bbs.wdzj.com/thread/getAuthorComment?type=1&author=&tid={0}&page=1&page_size=50'.format(tid)
            yield Request(url, meta={"tid": tid, "current_page":1}, callback=self.parse_PostReply)





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
            item['CollectionTime']=get_time()
            yield item












