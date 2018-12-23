# coding=utf-8
import re
import datetime
from collections import Iterator
import telnetlib
import requests
import logging
from lxml import etree
# logging.basicConfig(level=logging.DEBUG)

def change_to_single_quote(txt):
    return txt.replace('"', "'")

def remove_space(txt):
    txt = change_to_single_quote(txt)
    return txt.strip().replace("\n","").replace("\t","").replace(" ","")

def clean_values(values):
    if isinstance(values, float):
        return values
    if isinstance(values, int):
        return values
    if values is None:
        return ""
    elif isinstance(values, str):
        values="".join(values.split())
        return remove_space(values)
    elif isinstance(values, Iterator):
        if len(list)==1:
            return remove_space(str(values[0]))
        else:
            return values




def get_time():
    """
    返回当前时间
    :return:
    """
    return str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))


def remove_slash(txt):
    return txt.replace("\\","")





def remove_tags(txt):
    try:
        selector=etree.HTML(txt)
        msg =selector.xpath("string(.)")
        txt= "".join(msg.split())
    except Exception as err:
        logging.error(err)
    return clean_values(txt)
