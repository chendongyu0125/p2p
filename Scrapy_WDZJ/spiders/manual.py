# -*- coding: utf-8 -*-
import scrapy
from Scrapy_WDZJ.items import ScrapyWdzjItem
from scrapy.loader.processors import MapCompose, Join
from scrapy.loader import ItemLoader
import datetime
from scrapy.http import Request
from urllib.parse import urljoin

class BasicSpider(scrapy.Spider):
    name = 'manual'
    allowed_domains = ['wdzj']
    start_urls = ['https://www.wdzj.com/dangan/search?currentPage=1']

    def parse_item(self, response):
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
    def parse(self, response):
        next_pagenum = response.xpath("//a[@class='pageindex' and text()='下一页']/@currentnum").extract()[-1]
        url_base = "https://www.wdzj.com/dangan/search?currentPage={0}"
        next_url = url_base.format(next_pagenum)

        # yield self.parse_item(response)







        for url in [next_url]:
            yield Request(url,dont_filter=True)
        #
        # for url in [response.url]:
        #     yield Request(url, callback=self.parse_item, dont_filter=True







