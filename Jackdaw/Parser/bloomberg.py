
from Jackdaw.Parser.Utils import *
from typing import List
from lxml import etree
import json

url_head = ["www.bloomberg.com/news/articles/", 'www.bloomberg.com/news/videos/',
    'www.bloomberg.com/news/audio/',
 'www.bloomberg.com/news/features/', 'www.bloomberg.com/news/newsletters/',
 'www.bloomberg.com/news/', 'www.bloomberg.com/video/', 'www.bloomberg.com/opinion/articles/',
 'www.bloomberg.com/graphics/', 'www.bloomberg.com/features/', 'www.bloomberg.com/opinion/features/']
def cleanurl(url) -> str:
    return clean_url(url, url_head)

time_parser_list = [
    (" hours ago", "hours"), (" hour ago", "hours"), (" minutes ago", "minutes"), (" minute ago", "minutes"), 
    (" min ago", "minutes")]
def parse_time_text(time: str, snapshot_date: datetime) -> datetime:
    if time.startswith("Updated "):
        time = time[len("Updated "):]
    for txt, key in time_parser_list:
        if time.endswith(txt):
            args = {key: int(time[:-len(txt)])}
            return snapshot_date - timedelta(**args)
    raise RuntimeError("unknwon time " + time)

badcount = 0
def parse_bloomberg_topics_china_2015(tree, snapshot_date: datetime) -> List[Document]:
    # first = tree.xpath('//div[@class="firstArticle item-container"]')
    # doassert(len(first) == 1, "head err"+str(first))
    items = tree.xpath('/html/body/script/text()')
    for script_str in items:
        idx = script_str.find("bootstrappedData: ")
        if idx == -1:
            continue
        jsondata = script_str[idx+len("bootstrappedData: "): script_str.rfind("});")]
        if jsondata.endswith("};"):
            jsondata = jsondata[:-2]
        ret = []
        try:
            data = json.loads(jsondata)
            if len(data) == 0:
                global badcount
                badcount += 1
                print("Bad page: ", snapshot_date)
                if badcount <= 3:
                    return []
                else:
                    raise RuntimeError()
            for item in data['/api/topics/china']['items']:
                ts = datetime.fromisoformat(item['publishedAt'].replace("Z", '+00:00')).astimezone(beijing)
                title = item['headline']
                content = " "
                link = cleanurl(item['url'])
                ret.append(Document(ts, title, content, link))
        except Exception as e:
            with open("out/err.json", 'w', encoding="utf-8") as f:
                f.write(jsondata)
            raise e
        print(ret)
        return ret
    raise RuntimeError("Bad page")



def parse_bloomberg_news_china_2012(tree, snapshot_date: datetime) -> List[Document]:
    items = tree.xpath('//div[@class="news_item news_headline_summary exclusive  clearfix"]')
    items += tree.xpath('//div[@class="news_item news_headline_summary exclusive clearfix"]')
    doassert(len(items) >=3, "items err")
    ret = []
    for item in items:
        link = doassert_single(item.xpath('.//div[@class="q_link_wrapper"]/a/@href'), "link err").strip()
        title = doassert_single(item.xpath('.//div[@class="q_link_wrapper"]/a/text()'), "title err").strip()
        time = doassert_or_empty_str(item.xpath('.//span[@class="timestamp inline"]/text()'), "time err").strip()
        if time:
            ts = parse_time_text(time, snapshot_date)
        else:
            ts = snapshot_date
        content = doassert_single(item.xpath('.//p/text()'), "content err").strip()
        ret.append(Document(ts, title, content, cleanurl(link)))
    items = tree.xpath('//div[@class="news_item exclusive clearfix bar"]')
    doassert(len(items) >=5, "items err")
    for item in items:
        link = doassert_single(item.xpath('.//a/@href'), "link err").strip()
        title = doassert_single(item.xpath('.//a/text()'), "title err").strip()
        time = doassert_or_empty_str(item.xpath('.//span[@class="timestamp inline"]/text()'), "time err").strip()
        if time:
            ts = parse_time_text(time, snapshot_date)
        else:
            ts = snapshot_date
        content = " "
        ret.append(Document(ts, title, content, cleanurl(link)))
    return ret


def parse_bloomberg_next_china_2019(tree, snapshot_date: datetime) -> List[Document]:
    items = tree.xpath('//a[@class="story-package-module__story__headline-link"]') + \
        tree.xpath('//a[@class="single-story-module__headline-link"]')
    doassert(len(items) >= 7, "items err" + str(len(items)))
    ret = []
    for item in items:
        ii: etree._Element = item
        link = cleanurl(ii.get("href"))
        title = ii.text.strip()
        # print(etree.tostring(item.getparent()))
        time = item.getparent().xpath('./time[@class="hub-timestamp" or @class="hub-timestamp hub-timestamp--iso"]/@datetime')
        if len(time) == 1:
            time = time[0].strip()
        else:
            doassert(len(time) == 0, "111")
            time = doassert_single(item.getparent().getparent().xpath('./time[@class="hub-timestamp" or @class="hub-timestamp hub-timestamp--iso"]/@datetime'), "TIME err" + title).strip()
        ts = datetime.fromisoformat(time.replace("Z", '+00:00')).astimezone(beijing)
        content = " "
        ret.append(Document(ts, title, content, link))
    return ret
