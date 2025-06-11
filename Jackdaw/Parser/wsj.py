
from Jackdaw.Parser.Utils import *
from typing import List
from lxml import etree
import json

time_parser_list = [
    (" hours ago", "hours"), (" hour ago", "hours"), (" minutes ago", "minutes"), (" minute ago", "minutes"), 
    (" min ago", "minutes")]
def parse_time_text(time: str, snapshot_date: datetime) -> datetime:
    for txt, key in time_parser_list:
        if time.endswith(txt):
            args = {key: int(time[:-len(txt)])}
            return snapshot_date - timedelta(**args)
    if "Just In" == time:
        return snapshot_date - timedelta(minutes=10)
    try:
        return ny_to_shanghai_time(datetime.strptime(time, "%b. %d, %Y").replace(hour=23, minute=59, second=59))
    except ValueError:
        pass
    return ny_to_shanghai_time(datetime.strptime(time, "%B %d, %Y").replace(hour=23, minute=59, second=59))

wsj_link_head = ["www.wsj.com/articles/", "blogs.wsj.com/", "www.wsj.com/world/china/", "www.wsj.com/world/"]
def clean_url(link: str) -> str:
    for linkhead in wsj_link_head:
        link_idx = link.find(linkhead)
        if link_idx != -1:
            link_idx = link_idx+len(linkhead)
            break
    doassert(link_idx!=-1, "bad link:"+link)
    short_link = link[link_idx:]
    idx = short_link.find("?")
    if idx != -1:
        short_link = short_link[:idx]
    if short_link[-1] == "/":
        short_link=short_link[:-1]
    return short_link

def parse_wsj_news_world_china_2015(tree, snapshot_date: datetime) -> List[Document]:
    # first = tree.xpath('//div[@class="firstArticle item-container"]')
    # doassert(len(first) == 1, "head err"+str(first))
    items = tree.xpath('//div[@class="headline-container"]')
    doassert(len(items) >=7, "items err")
    ret = []
    for item in items:
        link = doassert_single(item.xpath('.//h3[@class="headline"]/a/@href'), "link err").strip()
        time = doassert_single(item.xpath('.//div[@class="time-container"]/text()'), "time err").strip()
        time = parse_time_text(time, snapshot_date)
        title = doassert_single(item.xpath('.//h3[@class="headline"]/a/text()'), "link err").strip()
        content = doassert_or_empty_str(item.xpath('.//div[@class="summary-container"]/p/text()'), "link err").strip()
        ret.append(Document(time, title, content, clean_url(link)))
    return ret

def xpath_class_ends_with(s: str) -> str:
    return f"substring(@class, string-length(@class) - string-length('{s}') + 1) = '{s}'"

xpaths = [{
    "root": '//div[@class="css-1c2ypzx"]',
    "link": './/a[@class="epuzq557 css-q34s7v-Headline"]/@href',
    "time": './/p[@class="es486sg0 css-cw5wgv-TimeTag"]/text()',
    "title": './/a[@class="epuzq557 css-q34s7v-Headline"]/span/span/text()',
    "content": './/div[@class="epuzq556 css-16eq8mw-Summary"]/text()'
},
{
    "root": f'//div[{xpath_class_ends_with("StyledStack")}]',
    "link": f'.//a[{xpath_class_ends_with("HeadlineLink")}]/@href',
    "time": f'.//p[{xpath_class_ends_with("TimeTag")}]/text()',
    "title": f'.//a[{xpath_class_ends_with("HeadlineLink")}]/span//text()',
    "content": './/p[@class="css-lj39j"]/text()'
}
]
def parse_wsj_world_china_2023(tree, snapshot_date: datetime) -> List[Document]:
    jsondata = doassert_single(tree.xpath('//script[@id="__NEXT_DATA__"]/text()'), "script")
    d = json.loads(jsondata)
    cur = ""
    try:
        ret = []
        #dict_keys(['articleUrl', 'authors', 'flashline', 'headline', 'imageAlt', 'imageHeight', 'imageUrl', 'imageWidth', 'readTime', 'seoId', 'summary', 'timestamp']
        for ar in d["props"]['pageProps']['latestArticles'] + d["props"]['pageProps']['moreInArticlesInitial']:
            cur = ar
            if "isAd" in ar:
                continue
            if "timestamp" in ar:
                if "Z" in ar["timestamp"]:
                    ts = datetime.fromisoformat(ar["timestamp"].replace("Z", '+00:00')).astimezone(beijing)
                else:
                    ts = ny_to_shanghai_time(datetime.fromisoformat(ar["timestamp"]))
            else:
                ts = snapshot_date
            ret.append(Document(ts, ar["headline"], ar["summary"] if "summary" in ar else "", ar["seoId"]))
    except Exception as e:
        print(cur)
        with open("out/err.json", 'w', encoding="utf-8") as f:
            f.write(jsondata)
        raise e
    return ret

def _e(tree, snapshot_date: datetime):
    # first = tree.xpath('//div[@class="firstArticle item-container"]')
    # doassert(len(first) == 1, "head err"+str(first))
    for xpath in xpaths:
        items = tree.xpath(xpath["root"])
        if len(items) < 5:
            continue
        ret = []
        for item in items:
            try:
                link = doassert_single(item.xpath(xpath["link"]), "link err").strip()
                time = item.xpath(xpath["time"])

                time = doassert_single(item.xpath(xpath["time"]), "time err").strip()
                time = parse_time_text(time, snapshot_date)
                title = doassert_single([t for t in item.xpath(xpath["title"]) if ".css" not in t], "title err").strip()
                content = doassert_or_empty_str(item.xpath(xpath["content"]), "content err").strip()
                ret.append(Document(time, title, content, clean_url(link)))
            except Exception as e:
                print(etree.tostring(item).decode("utf-8"))
                print(snapshot_date)
                raise e
        return ret
    raise RuntimeError("unknown page")
    