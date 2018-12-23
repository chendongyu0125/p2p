# -*- coding: utf-8 -*-
import scrapy
from Scrapy_WDZJ.items import ScrapyWdzjItem
from scrapy.loader.processors import MapCompose, Join
from scrapy.loader import ItemLoader
import datetime

class BasicSpider(scrapy.Spider):
    name = 'basic'
    allowed_domains = ['wdzj']
    start_urls = ['https://www.wdzj.com/dangan/']

    def parse(self, response):
        """This function parses a property page.

        @url https://www.wdzj.com/dangan/
        @returns items 1
        @scrapes platformName platformStartDate
        @scrapes date
        """


        l = ItemLoader(item=ScrapyWdzjItem(), response=response)
        l.add_xpath('platformName', '//div[@class="itemTitle"]/h2/a/text()')
        l.add_xpath('platformStartDate','//*[@id="showTable"]/ul/li/div[2]/a/div[4]/text()',
                MapCompose(lambda i: i.replace(' ','')))
        l.add_value("date", datetime.datetime.now())
        return l.load_item()
        # item = ScrapyWdzjItem()
        # item['platformStatus']=response.status
        # item['platformName']=response.xpath("//div[@class='itemTitle']/h2/a/text()").extract()
        # item['platformStartDate']=response.xpath('//*[@id="showTable"]/ul/li/div[2]/a/div[4]/text()').extract()
        # return item
       # self.log("status : %d" % response.status)
       # self.log("platname: %s" % response.xpath("//div[@class='itemTitle']/h2/a/text()").extract())
       # self.log("start date: %s" % response.xpath('//*[@id="showTable"]/ul/li/div[2]/a/div[4]/text()').extract())

       #  # pass





