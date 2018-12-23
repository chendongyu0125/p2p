# -*- coding: utf-8 -*-
import scrapy
from scrapy.linkextractors import LinkExtractor
from scrapy.spiders import CrawlSpider, Rule, Spider
from scrapy.http import Request
from Scrapy_WDZJ.items import *
import time

class ProxyHunterSpider(Spider):
    name = 'proxy_hunter'
    # allowed_domains = ['xicidaili']
    start_urls = ['https://www.xicidaili.com/wn/']

    # rules = (
    #     Rule(LinkExtractor('//a[@class="next_page"]'), follow=True),
    #     Rule(LinkExtractor(allow=r'Items/'), callback='parse_item'),
    # )

    def parse(self, response):
        base = "https://www.xicidaili.com"
        next_pages = response.xpath('//a[@class="next_page"]/@href').extract()
        for next_page in next_pages:
            url = base + next_page
            time.sleep(25)
            yield Request(url)

        xpath_root = '//table[@id="ip_list"]/tr'
        selectors = response.xpath(xpath_root)
        for selector in selectors:
            item_loader = WDZJItemLoader(item = ValidProxy(), selector= selector)
            item_loader.add_xpath('ip', './td[2]/text()')
            item_loader.add_xpath('port', './td[3]/text()')
            item_loader.add_xpath('protocal', './td[6]/text()')
            item = item_loader.load_item()
            yield(item)

