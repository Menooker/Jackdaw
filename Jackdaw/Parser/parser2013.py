from Jackdaw.Utils import all_not_in, do_assert, trim_main_url
from typing import List, Literal
from lxml import etree

class Parser2013_v1:
    @staticmethod
    def is_it(url, tree, text):
        if tree is None:
            return "bad"
        if tree.xpath('//div[@data-sudaclick="blk_newsinter_01"]/ul').__len__() == 1 \
            and tree.xpath('//div[@data-sudaclick="blk_newsinland_01"]/ul').__len__() == 1:
            return "good"
        if "The Wayback Machine is an" in text and "response at crawl time" in text:
            return "skip"
        if tree.xpath('//p[@class="submit"]/a[text()="直接回旧版"]'):
            return "skip"
        for title in tree.xpath("//title/text()"):
            if "新浪财经iPad版" in title:
                return "skip"
            if "新浪财经_手机新浪网" in title:
                return "skip"
        return "bad"
    
    @staticmethod
    def parse_base(url: str, tree, base):
        parsed = tree.xpath(f'{base}/div[@data-sudaclick="blk_newsinter_01"]/ul/li/a/@href')
        do_assert(url, len(parsed) >= 3)
        urls = parsed
        parsed = tree.xpath('//div[@data-sudaclick="blk_newsinland_01"]/ul/li/a/@href')
        do_assert(url, len(parsed) >= 3)
        urls.extend(parsed)
        urls = [x for x in urls if all_not_in(["http://licaishi.sina.com.cn/web/videoLive", "www.yizhibo.com", "z.sina.com.cn", "rl.cj.sina.com.cn"], x)]
        return urls
    @staticmethod
    def parse(url: str, tree):
        return Parser2013_v1(url, tree, "/")


class Parser2024_v1:
    @staticmethod
    def is_it(url, tree, text):
        if tree.xpath('//div[@class="m-p-left fleft"]/div[@data-sudaclick="blk_newsinter_01"]/ul').__len__() == 1 \
            and tree.xpath('//div[@data-sudaclick="blk_newsinland_01"]/ul').__len__() == 1:
            return "good"
    @staticmethod
    def parse(url: str, tree):
        return Parser2013_v1.parse_base(url, tree, '//div[@class="m-p-left fleft"]')
