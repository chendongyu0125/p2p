# -*- coding: utf-8 -*-
import scrapy
from Scrapy_WDZJ.items import ScrapyWdzjItem, UserInfoItem
from scrapy.loader.processors import MapCompose, Join
from scrapy.loader import ItemLoader
import datetime

class BasicSpider(scrapy.Spider):
    name = 'userinfo'
    allowed_domains = ['wdzj']

    start_urls = ("https://member.wdzj.com/space-{0}.html".format(userid) for userid in range(1,1790000))

    error_pagenum=0

    def parse(self, response):
        """This function parses a property page.

          @url https://member.wdzj.com/space-1.html
          @returns items 1
          @scrapes userID userName p_level role score
          @scrapes Num_Friends Num_Platforms Num_Fans Num_ColumnWriters Num_Collections
          @scrapes date

          """
        if self.error_pagenum>=50: # 如果连续50次没有抓取到正确的页面，则停止
            return

        if response.status == 200:
            self.error_pagenum=0

            item = UserInfoItem()

            # basic informaton
            try:

                item['userID'] =response.xpath('//div[contains(@class,"p-top-user")]/a/@href').re('https://member.wdzj.com/space-([0-9]+).html')[0]
                item['userName']=response.xpath('//div[contains(@class,"p-top-user")]/a/text()').extract()[0]
                item['p_level'] =response.xpath('//span[@class="p-lev"]/text()').extract()[0]
                item['role'] =response.xpath("//div[@class='mb20']//a[1]/text()").extract()[0]
                item['score'] =response.xpath("//i[@class='noStatic orange']/text()").re('\(([0-9]+)\s+积分\)')[0]

                # activity information
                item['Num_Friends'] =response.xpath("//div[contains(@class,'p-nav-list')]//a[contains(@href,'type=touy')]/p/text()").extract()[0]
                # item['Num_Platforms'] =response.xpath("//div[contains(@class,'p-nav-list')]//a[contains(@href,'type=plat')]/p/text()").extract()[0]
                item['Num_Fans'] =response.xpath("//div[contains(@class,'p-nav-list')]//a[contains(@href,'type=fans')]/p/text()").extract()[0]
                item['Num_ColumnWriters'] =response.xpath("//div[contains(@class,'p-nav-list')]//a[contains(@href,'type=author')]/p/text()").extract()[0]
                item['Num_Collections'] =response.xpath("//div[contains(@class,'p-nav-list')]//a[text()='收藏']/p/text()").extract()[0]

                # House Keeping information
                item['date'] = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                return item

            except Exception as err:
                pass


        else:
            self.error_pagenum=self.error_pagenum+1



    # def parse(self, response):
    #
    #     next_user =







