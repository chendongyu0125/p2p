# -*- coding: utf-8 -*-

# Define here the models for your scraped items
#
# See documentation in:
# https://doc.scrapy.org/en/latest/topics/items.html

import scrapy
from scrapy import Item, Field
from Scrapy_WDZJ.tools.dbtools import *
from Scrapy_WDZJ.tools.strtools import *
from scrapy.loader import ItemLoader
from scrapy.loader.processors import TakeFirst, MapCompose, Join, Identity

class WDZJItemLoader(ItemLoader):
    default_output_processor = TakeFirst()

class ScrapyWdzjItem(Item):
    # define the fields for your item here like:
    # name = scrapy.Field()
    platformStatus=Field() #返回状态
    platformName=Field() # 平台名称
    platformStartDate=Field() #平台上线时间
    date=Field()

    pass

class TouyouItem(Item):
    # encoding: utf-8
    # 数据编码： utf-8
    userID = Field() #当前用户ID
    # userName = Field()
    friendshipType=Field()  #社交关系类型：Touy，即投友
    touyouID = Field()      # 投友ID
    touyouName = Field()    # 投友昵称
    collectionDate = Field()  #数据收集时间

class FanItem(Item):
    # encoding: utf-8
    # 数据编码： utf-8
    userID = Field() #当前用户ID
    friendshipType=Field() #社交关系类型：粉丝
    fanID = Field()        # 粉丝ID
    fanUserName=Field()    # 粉丝昵称
    collectionDate = Field()  # 数据收集时间


class UserInfoItem(Item):

    # basic informaton
    userID = Field()  #用户编号，数字
    userName = Field() #用户ID昵称
    p_level = Field() #用户等级，如v1，v2，管理员，禁止发言等
    role = Field()  #用户身份，如投资人
    score = Field() #用户积分

    # activity information
    Num_Friends = Field() # 投友数量
    # Num_Platforms = Field() #关注的平台数量
    Num_Fans = Field() # 粉丝数量
    Num_ColumnWriters = Field() # 专栏作家文章数
    Num_Collections = Field() #收藏


    # House Keeping information
    date = Field() #抓取数据的时间


class PlatformSnapshot(Item):
    """
    平台概览
    打开 https://www.wdzj.com/dangan/ 看到的概览信息
    """


    platNamePin = Field()
    platStatus = Field()
    platName = Field()
    platEarningsCode = Field(
        input_processor=MapCompose(parse_number)
    )
    money_stock = Field(

    )
    location = Field()
    onlineDate = Field()
    platReviewTag = Field(
        output_processor=Identity()
    )
    CreditRanking = Field(

    )
    LabelList = Field()
    OverallScore = Field(

    )
    NumberOfCommentators = Field(

    )
    CollectionTime = Field()

    def initValue(self):
        init_item(self)


    # def save(self, cursor, table_name=""):
    #     if table_name=="":
    #         table_name=self.__class__.__name__
    #     values = ['"'+str(self[var])+'"' for var in self.fields.keys()]
    #     sql = "insert into {0} ({1}) values ({2})".format(table_name, ', '.join(self.fields.keys()), ', '.join(values))
    #     cursor.execute(sql)

    def save(self, cursor, conn):
        save_item(self, cursor, conn)

class OperationPerformanceDaily(Item):
    platId = Field()
    ListingAmountDistribution_0_20W = Field()
    ListingAmountDistribution_20_100W = Field()
    ListingAmountDistribution_Above100W = Field()
    ListingTypeDistribution_BidTransfer = Field()
    ListingTypeDistribution_WebCredit = Field()
    ListingTypeDistribution_SiteCredit = Field()
    ListingTermDistribution_0_3M = Field()
    ListingTermDistribution_3_6M = Field()
    ListingTermDistribution_6_12M = Field()
    ListingTermDistribution_Above12M = Field()
    AmountValue = Field()
    InflowValue = Field()
    MoneyStockValue = Field()
    PlatformReturn = Field()
    AvgListingTerm = Field()
    NumberOfBidders = Field()
    AvgAmountOfBidders = Field()
    NumberOfBiddersToBeRepaid = Field()
    NumberOfBorrowers = Field()
    AvgAmountOfBorrowers = Field()
    NumberOfBids = Field()
    NumberofBiddersToBePaid = Field()
    AmountValue_RankingAbove = Field()
    InflowValue_RankingAbove = Field()
    MoneyStockValue_RankingAbove = Field()
    PlatformReturn_RankingAbove = Field()
    AvgListingTerm_RankingAbove = Field()
    NumberOfBidders_RankingAbove = Field()
    AvgAmountOfBidders_RankingAbove = Field()
    NumberOfBiddersToBeRepaid_RankingAbove = Field()
    NumberOfBorrowers_RankingAbove = Field()
    AvgAmountOfBorrowers_RankingAbove = Field()
    NumberOfBids_RankingAbove = Field()
    NumberofBiddersToBePaid_RankingAbove = Field()
    CollectionTime = Field()

    def initValue(self):
        """
        数据初始化为空字符串
        :return:
        """
        init_item(self)

    def save(self,cursor, conn):
        """
        保存数据
        :param cursor:
        :return:
        """
        save_item(self, cursor, conn)

class OperationPerformanceMonthly(Item):
    platId = Field()
    amount = Field()
    bidder_num = Field()
    borrower_num = Field()
    bor_top10 = Field()
    bid_top10 = Field()
    net_inflow = Field()
    amount_percent = Field()
    bidder_num_percent = Field()
    borrower_num_percent = Field()
    bor_top10_change = Field()
    bid_top10_change = Field()
    net_inflow_percent = Field()
    CollectionTime = Field()
    def initValue(self):
        """
        数据初始化为空字符串
        :return:
        """
        init_item(self)

    def save(self,cursor, conn):
        """
        保存数据
        :param cursor:
        :return:
        """
        save_item(self, cursor, conn)

class AbnormalOperationHistory(Item):
    """
    异常经营信息
    """
    platId = Field()
    SerialNumber = Field()
    InListReason = Field()
    InlistDate = Field()
    InListInstitution = Field()
    RemoveListReason = Field()
    RemoveListDate = Field()
    RemoveListInstitution = Field()
    CollectionTime = Field()
    def initValue(self):
        """
        数据初始化为空字符串
        :return:
        """
        init_item(self)

    def save(self, cursor, conn):
        """
        保存数据
        :param cursor:
        :return:
        """

        save_item(self, cursor, conn)


class CompanyChangeHistory(Item):
    """
    公司变更信息
    """
    platId = Field()
    ChangeDate = Field()
    ChangeType = Field()
    InfoBeforeChange = Field()
    InfoAfterChange = Field()
    CollectionTime = Field()
    def initValue(self):
        """
        数据初始化为空字符串
        :return:
        """
        init_item(self)

    def save(self, cursor, conn):
        """
        保存数据
        :param cursor:
        :return:
        """
        save_item(self, cursor, conn)

class CapitalStructure(Item):
    """
    股权信息
    """

    platId = Field()
    ShareholderName = Field()
    SharePercent = Field(
        input_processor=MapCompose(parse_number)
    )
    SubscribedCapitalContribution = Field(
        input_processor=MapCompose(parse_number)
    )
    CollectionTime = Field()
    def initValue(self):
        """
        数据初始化为空字符串
        :return:
        """
        init_item(self)

    def save(self, cursor, conn):
        """
        保存数据
        :param cursor:
        :return:
        """
        save_item(self, cursor, conn)


class TopManager(Item):

    platId = Field()
    Name = Field()
    Position = Field()
    Description = Field()
    ImageURL = Field()
    CollectionTime = Field()

    def initValue(self):
        """
        数据初始化为空字符串
        :return:
        """
        init_item(self)

    def save(self, cursor, conn):
        """
        保存数据
        :param cursor:
        :return:
        """
        save_item(self, cursor, conn)


class CompanyBackground_Part1(Item):
    """
    公司注册信息

    """

    platId = Field()
    RegisterStatus = Field()
    RegisterInstitution = Field()
    DomainName = Field()
    RegisterCompanyName = Field()
    OpenDate = Field()
    RunningDate = Field()
    ApprovedDate = Field()
    PutInRecordDate = Field()
    CompanyNature = Field()
    BusinessScope = Field()
    RegisterCapital = Field(
        input_processor=MapCompose(parse_number)
    )
    UnifiedSocialCreditNumber = Field()
    CollectionTime = Field()

    def initValue(self):
        """
        数据初始化为空字符串
        :return:
        """
        init_item(self)

    def save(self, cursor, conn):
        """
        保存数据
        :param cursor:
        :return:
        """
        save_item(self, cursor, conn)


class CompanyBackground_Part2(Item):

    platId = Field()
    Fax = Field()
    ZipCode = Field()
    CompanyDescription = Field()
    ICPNumber = Field()
    Email = Field()
    CorrespondenceAddress = Field()
    CollectionTime = Field()

    def initValue(self):
        """
        数据初始化为空字符串
        :return:
        """
        init_item(self)

    def save(self, cursor, conn):
        """
        保存数据
        :param cursor:
        :return:
        """
        save_item(self, cursor, conn)

class PlatformDetail(Item):

    platId = Field()
    platCode = Field()
    platNamePin = Field()
    platName = Field()
    platBackgroundDetailExpand = Field()
    platStatus = Field()
    problemTime = Field()
    onlineDate = Field()
    locationArea = Field()
    locationAreaName = Field()
    locationCity = Field()
    locationCityName = Field()
    registeredCapital = Field()
    bankFunds = Field()
    bankCapital = Field()
    association = Field()
    associationDetail = Field()
    autoBid = Field()
    newTrustCreditor = Field()
    bidSecurity = Field()
    securityModel = Field()
    securityModelCode = Field()
    securityModelOther = Field()
    companyName = Field()
    juridicalPerson = Field()
    businessType = Field()
    officeAddress = Field()
    recordId = Field()
    oldPlatName = Field()
    actualCapital = Field()
    manageExpense = Field()
    manageExpenseDetail = Field()
    withdrawExpense = Field()
    withdrawExpenseDetail = Field()
    rechargeExpense = Field()
    rechargeExpenseDetail = Field()
    transferExpense = Field()
    transferExpenseDetail = Field()
    vipExpense = Field()
    vipExpenseDetail = Field()
    serviceTel = Field()
    platBackgroundExpandChild = Field()
    servicePhone = Field()
    equityVoList = Field()
    recordLicId = Field()
    riskcontrolDetail = Field()
    riskcontrolDetailArray = Field()
    payment = Field()
    paymode = Field()
    riskcontrol = Field()
    credit = Field()
    inspection = Field()
    problem = Field()
    platEquity = Field()
    platIsStatic = Field()
    displayFlg = Field()
    bindingFlag = Field()
    showShuju = Field()
    tzjPj = Field()
    gjlhhTime = Field()
    withTzj = Field()
    gjlhhFlag = Field()
    platBackground = Field()
    platBackgroundExpand = Field()
    platBackgroundDetail = Field()
    platBackgroundMark = Field()
    riskFunds = Field()
    riskCapital = Field()
    fundCapital = Field()
    trustFunds = Field()
    trustCapital = Field()
    trustCreditor = Field()
    riskReserve = Field()
    trustCreditorMonth = Field()
    gruarantee = Field()
    CollectionTime = Field()

    def initValue(self):
        """
        数据初始化为空字符串
        :return:
        """
        init_item(self)

    def save(self, cursor, conn):
        """
        保存数据
        :param cursor:
        :return:
        """
        save_item(self, cursor, conn)

class DailyOperation_Part1(Item):
    platId = Field()
    date = Field()
    amountValue = Field()
    bidValue = Field()
    borValue = Field()
    netFlowValue = Field()
    moneyStockValue_y1 = Field()
    moneyStockValue_y2 = Field()
    CollectionTime = Field()

    def initValue(self):
        """
        数据初始化为空字符串
        :return:
        """
        init_item(self)

    def save(self, cursor, conn):
        """
        保存数据
        :param cursor:
        :return:
        """
        save_item(self, cursor, conn)


class DailyOperation_Part2(Item):

    platId = Field()
    date = Field()
    platformReturn = Field()
    amountValue = Field() #成交量
    CollectionTime = Field()

    def initValue(self):
        """
        数据初始化为空字符串
        :return:
        """
        init_item(self)

    def save(self, cursor, conn):
        """
        保存数据
        :param cursor:
        :return:
        """
        save_item(self, cursor, conn)

class DailyOperation_Part3(Item):

    platId = Field()
    date = Field()
    bidValuePerCapita = Field()
    borValuePerCapita = Field()
    CollectionTime = Field()

    def initValue(self):
        """
        数据初始化为空字符串
        :return:
        """
        init_item(self)

    def save(self, cursor, conn):
        """
        保存数据
        :param cursor:
        :return:
        """
        save_item(self, cursor, conn)

class DailyOperation_Part4(Item):

    platId = Field()
    date = Field()
    newBidders = Field()
    oldBidders = Field()
    CollectionTime = Field()

    def initValue(self):
        """
        数据初始化为空字符串
        :return:
        """
        init_item(self)

    def save(self, cursor, conn):
        """
        保存数据
        :param cursor:
        :return:
        """
        save_item(self, cursor, conn)

class DailyOperation_Part5(Item):

    platId = Field()
    date = Field()
    newBidderAmount = Field()
    oldBidderAmount = Field()
    CollectionTime = Field()

    def initValue(self):
        """
        数据初始化为空字符串
        :return:
        """
        init_item(self)

    def save(self, cursor, conn):
        """
        保存数据
        :param cursor:
        :return:
        """
        save_item(self, cursor, conn)

class PlatformEvaluation_Part1(Item):

    platId = Field()
    good = Field()
    midd = Field()
    bad = Field()
    CollectionTime = Field()

    def initValue(self):
        """
        数据初始化为空字符串
        :return:
        """
        init_item(self)

    def save(self, cursor, conn):
        """
        保存数据
        :param cursor:
        :return:
        """
        save_item(self, cursor, conn)

class PlatformReview(Item):

    platId = Field()
    platName = Field()
    platPin = Field()
    reviewId = Field()
    reviewUserName = Field()
    reviewUserId = Field()
    reviewDate = Field()
    reviewEvaluation = Field()
    salaryguard = Field()
    withdrawSpeed = Field()
    websiteExperience = Field()
    serviceAttitude = Field()
    tagList = Field()
    reviewContent = Field()
    useful = Field()
    noUseful = Field()
    parentReviewPlatFlag = Field()
    orderType = Field()
    reviewPlatFlag = Field()
    hasReply = Field()
    CollectionTime = Field()

    def initValue(self):
        """
        数据初始化为空字符串
        :return:
        """
        init_item(self)

    def save(self, cursor, conn):
        """
        保存数据
        :param cursor:
        :return:
        """
        save_item(self, cursor, conn)
class PlatformReviewReply(Item):
    """

    """
    orderType = Field()
    parentReviewId = Field()
    parentReviewPlatFlag = Field()
    parentReviewUserId = Field()
    parentReviewUserName = Field()
    reviewContent = Field()
    reviewDate = Field()
    reviewId = Field()
    reviewPlatFlag = Field()
    reviewUserId = Field()
    reviewUserName = Field()
    CollectionTime = Field()

    def initValue(self):
        """
        数据初始化为空字符串
        :return:
        """
        init_item(self)

    def save(self, cursor, conn):
        """
        保存数据
        :param cursor:
        :return:
        """
        save_item(self, cursor, conn)


class PlatformEvaluation_Part2(Item):

    platId = Field()
    drawScore = Field()
    delayScore = Field()
    serviceScore = Field()
    experienceScore = Field()
    drawScoreDetail = Field()
    delayScoreDetail = Field()
    serviceScoreDetail = Field()
    experienceScoreDetail = Field()
    CollectionTime = Field()

    def initValue(self):
        """
        数据初始化为空字符串
        :return:
        """
        init_item(self)

    def save(self, cursor, conn):
        """
        保存数据
        :param cursor:
        :return:
        """
        save_item(self, cursor, conn)

class PlatformEvaluation_Part3(Item):

    platId = Field()
    ReviewScore_Overall = Field(
        input_processor=MapCompose(parse_number)
    )
    ReviewersNumber = Field(
        input_processor=MapCompose(parse_number)
    )
    Comparision_Withdraw = Field()
    Comparison_Idle = Field()
    Comparison_Service = Field()
    Comparison_Experience = Field()
    OverallRanking = Field(
        input_processor=MapCompose(parse_number)
    )
    OverallScore = Field(
        input_processor=MapCompose(parse_number)
    )
    NumberOfWatchers = Field(
        input_processor=MapCompose(parse_number)
    )
    CollectionTime = Field()

    def initValue(self):
        """
        数据初始化为空字符串
        :return:
        """
        init_item(self)

    def save(self, cursor, conn):
        """
        保存数据
        :param cursor:
        :return:
        """
        save_item(self, cursor, conn)

class PostSnapshot(Item):

    platId = Field()
    AuthorID = Field()
    tid = Field()
    author_name = Field()
    terminal = Field()
    PostingDate = Field()
    PostAbstract = Field()
    PostTitle = Field()
    CollectionTime = Field()

    def initValue(self):
        """
        数据初始化为空字符串
        :return:
        """
        init_item(self)

    def save(self, cursor, conn):
        """
        保存数据
        :param cursor:
        :return:
        """
        save_item(self, cursor, conn)

class PostDetail(Item):

    tid = Field()
    AuthorID = Field()
    PostTitle = Field()
    PostingDate = Field()
    Content = Field()
    NumberOfComments = Field()
    Recommend_Yes = Field()
    Recommend_No = Field()
    NumberOfReaders = Field()
    PostType = Field()
    CollectionTime = Field()

    def initValue(self):
        """
        数据初始化为空字符串
        :return:
        """
        init_item(self)

    def save(self, cursor, conn):
        """
        保存数据
        :param cursor:
        :return:
        """
        save_item(self, cursor, conn)

class PostReply(Item):

    id = Field()
    tid = Field()
    author_id = Field()
    author_name = Field()
    terminal = Field()
    message = Field()
    create_time = Field()
    support = Field()
    fid = Field()
    parent_uid = Field()
    path = Field()
    is_top = Field()
    position = Field()
    attachment = Field()
    CollectionTime = Field()

    def initValue(self):
        """
        数据初始化为空字符串
        :return:
        """
        init_item(self)

    def save(self, cursor, conn):
        """
        保存数据
        :param cursor:
        :return:
        """
        save_item(self, cursor, conn)

class ValidProxy(Item):
    """
    存放有效的的代理服务器地址
    """

    ip = Field()
    port = Field()
    protocal =Field()
    valid = Field()
    verified = Field()
    valid_proxy_url=Field()
    CollectionTime = Field()
    def save(self, cursor, conn):
        """
        保存数据
        :param cursor:
        :return:
        """
        save_item(self, cursor, conn)
