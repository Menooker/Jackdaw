from Jackdaw.Utils import all_not_in, do_assert, trim_main_url
from typing import List, Literal
from lxml import etree

class Parser2013_v1:
    @staticmethod
    def is_it(url, tree, text):
        for title in tree.xpath("//title/text()"):
            if "新浪财经iPad版" in title:
                return "skip"
        if tree.xpath('//div[@data-sudaclick="blk_newsinter_01"]/ul').__len__() == 1 \
            and tree.xpath('//div[@data-sudaclick="blk_newsinland_01"]/ul').__len__() == 1:
            return "good"
        return "bad"
    
    @staticmethod
    def parse(url: str, tree):
        parsed = tree.xpath('//div[@data-sudaclick="blk_newsinter_01"]/ul/li/a/@href')
        do_assert(url, len(parsed) >= 3)
        urls = parsed
        parsed = tree.xpath('//div[@data-sudaclick="blk_newsinland_01"]/ul/li/a/@href')
        do_assert(url, len(parsed) >= 3)
        urls.extend(parsed)
        return urls
