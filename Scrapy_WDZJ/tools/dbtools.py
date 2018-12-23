# coding=utf-8
import os
import re
from Scrapy_WDZJ.tools.strtools import *
import logging
import sqlite3

def save_item(item, cursor, conn):
    table_name = item.__class__.__name__
    for itemkey in item.fields.keys():  #有些值没有填充，用空值代替
        if itemkey not in item.keys():
            item[itemkey] = ""
    values = ['"' + str(item[var]) + '"' for var in item.fields.keys()]
    sql = "insert into {0} ({1}) values ({2})".format(table_name, ', '.join(item.fields.keys()),
                                                      ', '.join(values))
    logging.info(sql)
    try:
        cursor.execute(sql)
        conn = cursor.connection
        conn.commit()
    except Exception as err:
        logging.error(err)
    cursor.close()
    conn.close()


def create_db(item, cursor):
    sql=gen_create_table_sql(item)
    cursor.execute(sql)

def init_item(item):
    for var in item.fields.keys():
        item[var] = ""

def save_image(item, res):
    file_path="imgs/"+item['platId']+"/"
    if not os.path.exists(file_path):
        os.makedirs(file_path)
    file_name = item['Name']+"_"+item['Position']+"_" + os.path.basename(item['ImageURL'])
    try:
        file_name=file_path+file_name
        if not os.path.exists(file_name): #如果图片不存在，就保存该图片
            file = open(file_name, 'wb')
            file.write(res.content)
            file.close()
    except Exception as err:
        pass

def parse_number(txt):
    match_re = re.match(".*?([.0-9]+).*", txt)
    if match_re:
        output = match_re.group(1)
    else:
        output = ""
    return output

def parse_digits(txt):
    match_re = re.match(".*?([0-9]+).*", txt)
    if match_re:
        output = match_re.group(1)
    else:
        output = ""
    return output

def push_item(dict_value, item):
    for key in dict_value.keys():
        if key not in item.fields.keys():
            raise Exception
    for var in dict_value.keys():
        item[var] = dict_value[var]
    return item

def load_item(dict_item, target_item):
    for var in dict_item.keys():
        target_item[var]=clean_values(dict_item[var])
    return target_item



def gen_insert_item_sql(item, table_name=""):
    """
    根据item内容插入表
    :param table_name:
    :param item:
    :return:
    """
    if table_name=="":
        table_name=item.__class__.__name__
    values = ['"' + item[var] + '"' for var in item.fields.keys()]  #所有存储到数据库中的字符串内层用'引号，外层用双引号
    sql = "insert into {0} ({1}) values ({2})".format(table_name, ', '.join(item.fields.keys()),
                                                          ', '.join(values))
    return sql


def gen_create_table_sql(item, table_name=""):
    """
    根据item信息创建表
    :param table_name:
    :param item:
    :return:
    """
    if table_name=="":
        table_name=item.__class__.__name__
    var_list = [var for var in item.fields.keys()]
    var_str = " char, ".join(var_list)
    var_str += " char"
    sql = "create table if not exists {0} ( {1} )".format(table_name, var_str)
    return sql
