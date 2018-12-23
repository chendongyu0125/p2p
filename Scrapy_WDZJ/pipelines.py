# -*- coding: utf-8 -*-

# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://doc.scrapy.org/en/latest/topics/item-pipeline.html

import sqlite3
import datetime
from Scrapy_WDZJ.items import *
from Scrapy_WDZJ.tools.dbtools import *
from Scrapy_WDZJ.tools.strtools import *

class ScrapyWdzjPipeline(object):
    def __init__(self, sqlite_file):
        self.sqlite_file=sqlite_file


    @classmethod
    def from_crawler(cls, crawler):
        ts=str(datetime.datetime.now().strftime("%Y-%m-%d"))
        return cls(
            sqlite_file=crawler.settings.get('SQLITE_FILE')+ts+".db"  # 从settings.py获取
        )

    def open_spider(self,spider):
        self.create_table()

    def create_table(self):
        self.connect_sql()
        c=create_db   # 使用缩写更开心
        s=self.cursor
        create_db(PlatformSnapshot(), self.cursor)
        create_db(CompanyBackground_Part1(), self.cursor)
        create_db(CompanyChangeHistory(), self.cursor)
        create_db(AbnormalOperationHistory(), self.cursor)
        c(CapitalStructure(), s)
        c(TopManager(), s)
        c(CompanyBackground_Part2(), s)
        c(PlatformDetail(),s)
        c(OperationPerformanceDaily(),s)
        c(OperationPerformanceMonthly(),s)
        c(DailyOperation_Part1(),s)
        c(DailyOperation_Part2(),s)
        c(DailyOperation_Part3(),s)
        c(DailyOperation_Part4(),s)
        c(DailyOperation_Part5(), s)
        c(PlatformEvaluation_Part1(),s)
        c(PlatformEvaluation_Part2(),s)
        c(PlatformEvaluation_Part3(), s)
        c(PostSnapshot(), s)
        c(PostDetail(), s)
        c(PostReply(), s)
        c(PlatformReview(), s)
        c(PlatformReviewReply(), s)
        c(ValidProxy(), s)


    def connect_sql(self):
        self.conn=sqlite3.connect(self.sqlite_file)
        self.cursor = self.conn.cursor()

    # def close_sql(self):
    #     self.conn.commit()
    #     self.cursor.close()
    #     self.conn.close()


    # def close_spider(self,spider):
    #     self.close_sql()

    def process_item(self, item, spider):
        item['CollectionTime']=get_time()
        self.conn=sqlite3.connect(self.sqlite_file)
        self.cursor = self.conn.cursor()
        item.save(self.cursor, self.conn)
        # self.close_sql()
        return item
