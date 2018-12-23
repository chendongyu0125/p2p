# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule


class UnittestSpider(CrawlSpider):
    name = 'unitTest'
    allowed_domains = ['wdzj']
    platId='40'
    start_urls = ['https://shuju.wdzj.com/plat-info-initialize.html']
    post_data = {"wdzjPlatId": platId}

    rules = (
        Rule(LinkExtractor(allow=r'Items/'), callback='parse_item', follow=True),
    )

    def parse_item(self, response):
        i = {}
        #i['domain_id'] = response.xpath('//input[@id="sid"]/@value').extract()
        #i['name'] = response.xpath('//div[@id="name"]').extract()
        #i['description'] = response.xpath('//div[@id="description"]').extract()
        return i
