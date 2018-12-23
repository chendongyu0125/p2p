# -*- coding: utf-8 -*-
import scrapy
from scrapy.loader.processors import MapCompose, Join
from scrapy.http import Request
from Scrapy_WDZJ.items import FanItem
import datetime
from scrapy.http import FormRequest
import json
from lxml import etree
import re

class BasicSpider(scrapy.Spider):
    name = 'Fans'
    allowed_domains = ['member.wdzj.com']
    # start_urls = ["https://member.wdzj.com/space-{0}.html".format(userid) for userid in range(1, 20)]
    # start_urls = ["https://member.wdzj.com/space-{0}.html".format(userid) for userid in range(1,1790000)]
    def start_requests(self):
        ajaxURL = "https://member.wdzj.com/space/ajaxMore"
        return [FormRequest(ajaxURL, formdata={"uid": str(userID), "type": "fans", "page": '1'},
                            callback= lambda response, formdata={"uid": str(userID), "type": "fans", "page": '1'}, ajaxURL=ajaxURL:
                            self.parse_AjaxMorePage(response, formdata, ajaxURL), dont_filter=True) for userID in range(1, 1790000)]


    # def parse_firstPage(self,response):
    #
    #     userID=response.xpath('//div[contains(@class,"p-top-user")]/a/@href').re('https://member.wdzj.com/space-([0-9]+).html')[0]
    #     # userName=response.xpath('//div[contains(@class,"p-top-user")]/a/text()').extract()[0]
    #     frienshipType="投友"
    #     touyous = response.xpath("//div[contains(@class,'p-platform') and contains(@class, 'p-touyou')]/ul/li")
    #     for touyou in touyous:
    #         touyouItem = TouyouItem()
    #         touyouName=touyou.xpath(".//div[contains(@class,'plt-uName')]/a/text()").extract()[0]
    #         touyouID=touyou.xpath(".//div[contains(@class,'plt-uName')]/a/@href").re("https://member.wdzj.com/space-([\d]+).html")[0]
    #
    #         touyouItem['userID']=userID
    #         # touyouItem['userName']=userName
    #         touyouItem['friendshipType']=frienshipType
    #         touyouItem['touyouID']=touyouID
    #         touyouItem['touyouName']=touyouName
    #         touyouItem['collectionDate']=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    #         yield touyouItem
    #
    #     # 如果页面中显示"查看更多"，则表明还有更多投友未显示
    #     showMorePage=response.xpath("//a[@class='pageMore' and contains(text(), '查看更多')]/text()")
    #
    #     if len(showMorePage)>0:
    #         ajaxURL="https://member.wdzj.com/space/ajaxMore"
    #         formdata={"uid": userID, "type":"touy", "page": '2'}
    #         yield FormRequest(ajaxURL, formdata=formdata, callback= lambda response, formdata=formdata, ajaxURL=ajaxURL: self.parse_AjaxMorePage(response, formdata, ajaxURL), dont_filter=True)
    #


    # def parse_morePage(self,response):
    def parse_AjaxMorePage(self, response, formdata, ajaxURL):  #将ajaxURL，以及当前用户ID等信息传递给处理函数

        userID = formdata['uid']
        currentPage=int(formdata['page'])
        friendshipType=formdata['type']

        js = json.loads(response.body)



        ajaxHtml = js['ajaxHtml'] # 如果没有返回任何数据，说明该用户没有投友
        if len(ajaxHtml) ==0:
            return

        htmlPage=js['htmlPage']

        selector = etree.HTML(ajaxHtml)
        fans = selector.xpath('//li')

        for fan in fans:

            fanNameInfo=fan.xpath(".//div[contains(@class,'plt-uName')]/a/text()") #如果包含的投友Name信息有效
            if len(fanNameInfo) >0:
                fanItem = FanItem()
                fanName = fan.xpath(".//div[contains(@class,'plt-uName')]/a/text()")[0]
                regx = re.compile(r"https://member.wdzj.com/space-([\d]+).html")
                fanID = regx.findall(fan.xpath(".//div[contains(@class,'plt-uName')]/a/@href")[0])[0]
                fanItem['userID'] = userID
                fanItem['friendshipType'] = friendshipType
                fanItem['fanID']=fanID
                fanItem['fanUserName']=fanName
                fanItem['collectionDate'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                yield fanItem
        if htmlPage=="show":  #说明还有更多页面
            formdata = {"uid": userID, "type": "fans", "page": str(currentPage + 1)}
            yield FormRequest(ajaxURL, formdata=formdata,
                              callback=lambda response, formdata=formdata, ajaxURL=ajaxURL: self.parse_AjaxMorePage(response, formdata, ajaxURL), dont_filter=True)





    #
    #
    # def parse(self, response):
    #     '''
    #          @url https://member.wdzj.com/space-1.html
    #          @returns items 1
    #          @scrapes userID, touyouID, friendshipType, collectionDate
    #
    #          :param response:
    #          :return:
    #     '''
    #     # 如果页面不存在，放弃
    #     userinfo = response.xpath('//div[contains(@class,"p-top-user")]/a/@href')
    #     if len(userinfo) <1 :
    #         return
    #
    #     # 如果投友数目为0， 放弃
    #     num_Touyou = MapCompose(float)(response.xpath("//div[contains(@class,'p-nav-list')]//a[contains(@href,'type=touy')]/p/text()").extract_first())[0]
    #     if num_Touyou < 1:
    #         return
    #     else:
    #         userID = response.xpath('//div[contains(@class,"p-top-user")]/a/@href').re('https://member.wdzj.com/space-([0-9]+).html')[0]
    #         url = "https://member.wdzj.com/space/more?type=touy&uid={0}"
    #         yield Request(url.format(userID), callback=self.parse_firstPage, dont_filter=True)



