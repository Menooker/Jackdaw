from Jackdaw.Utils import do_assert, trim_main_url
from typing import List, Literal
from lxml import etree

class Parser2012:
    @staticmethod
    def is_2012(tree, text, matchxpath) -> Literal["good", "bad", "skip"]:
        if tree.xpath(matchxpath).__len__() == 1:
            return "good"
        if "新浪提示您：普通用户手机建议选择" in text:
            return "skip"
        return "bad"
    @staticmethod
    def parse_2012(url: str, tree, matchxpath: str, tags: List[str]):
        # anchor = tree.xpath('//ul[@id="shFinanceX"]')
        # do_assert(url, anchor.__len__() == 1)
        # anchor: etree._Element = anchor[0]
        # query = anchor.xpath("li/a/@href")
        # do_assert(url, query)
        # urls: List[str] = query
        # query = anchor.getparent().xpath('div[@class="PicTxt"]/div[@class="Txt"]/h4/a/@href')
        # do_assert(url, query)
        # urls.extend(query)
        urls: List[str] = []
        # international
        def work(tag: str):
            anchor = tree.xpath(matchxpath.format(tag=tag))
            do_assert(url, anchor.__len__() == 1)
            anchor: etree._Element = anchor[0]
            anchor = anchor.getparent().getparent().getparent().getparent().xpath('div[@class="blk_14"]')
            do_assert(url, anchor.__len__() == 1)
            anchor: etree._Element = anchor[0]
            query = anchor.xpath('div[@class="PicTxt"]/div[@class="Txt"]/h4/a/@href')
            do_assert(url, query)
            urls.extend(query)

            query = anchor.xpath('ul[@class="list_009"]/li/a/@href')
            do_assert(url, query)
            urls.extend(query)
        for tag in tags:
            work(tag)
        if urls[0].endswith("world/index.shtml") or urls[0].endswith("finance.sina.com.cn/world/"):
            del urls[0]
        return urls
    

class Parser2012_v1:
    @staticmethod
    def is_it(url: str, tree, text: str):
        return Parser2012.is_2012(tree, text, '//h2[@class="Title_05 TS_05_01"]/span/a/img[@alt="国际"]')

    @staticmethod
    def parse(url: str, tree):
        return Parser2012.parse_2012(url, tree, '//h2[@class="Title_05 TS_05_01"]/span/a/img[@alt="{tag}"]', ["国际", "国内"])

class Parser2012_v2:
    @staticmethod
    def is_it(url: str, tree, text: str):
        base = trim_main_url(url)
        return Parser2012.is_2012(tree, text, f'//h2[@class="Title_05 TS_05_01"]/span/a[@href="https://web.archive.org/web/{base}/http://finance.sina.com.cn/china/"]')

    @staticmethod
    def parse(url: str, tree):
        base = trim_main_url(url)
        templ = f'//h2[@class="Title_05 TS_05_01"]/span/a[@href="https://web.archive.org/web/{base}/http://finance.sina.com.cn/{{tag}}/"]'
        return Parser2012.parse_2012(url, tree, templ, ["china", "world"])

class Parser2012_v3:
    @staticmethod
    def is_it(url, tree, text):
        if tree.xpath('//div[@class="news_inland"]//div[@class="news_inland_c"]').__len__() == 1:
            return "good"
        return "bad"
    
    @staticmethod
    def parse(url: str, tree):
        parsed = tree.xpath('//div[@class="news_inland"]//div[@class="news_inland_c"]/div/div/h4/a/@href')
        do_assert(url, len(parsed) == 1)
        urls = parsed
        parsed = tree.xpath('//div[@class="news_inland"]//div[@class="news_inland_c"]/ul/li/a/@href')
        do_assert(url, len(parsed) > 1)
        urls.extend(parsed)
        parsed = tree.xpath('//div[@class="news_inter"]//div[@class="news_inland_c"]/div/div/h4/a/@href')
        do_assert(url, len(parsed) == 1)
        urls.extend(parsed)
        parsed = tree.xpath('//div[@class="news_inter"]//div[@class="news_inland_c"]/ul/li/a/@href')
        do_assert(url, len(parsed) > 1)
        urls.extend(parsed)
        return urls
