
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

wsj_link_head = ["www.wsj.com/articles/", "blogs.wsj.com/", "www.wsj.com/world/china/", "www.wsj.com/world/", "online.wsj.com/article","www.wsj.com/article/"]
def clean_url_wsj(link: str) -> str:
    return clean_url(link, wsj_link_head)

def parse_wsj_news_world_china_2015(tree, snapshot_date: datetime) -> List[Document]:
    # first = tree.xpath('//div[@class="firstArticle item-container"]')
    # doassert(len(first) == 1, "head err"+str(first))
    items = tree.xpath('//div[@class="headline-container"]')
    if len(items) < 7:
        if tree.xpath('//div[@class="module trending_now"]//h2[@class="subhead"]/text()') == ["Most Popular Videos", "Most Popular Articles"]:
            print("Bad page", snapshot_date)
            return []
        raise RuntimeError("items err")
    # doassert(len(items) >=7, "items err")
    ret = []
    for item in items:
        link = doassert_single(item.xpath('.//h3[@class="headline"]/a/@href'), "link err").strip()
        time = doassert_single(item.xpath('.//div[@class="time-container"]/text()'), "time err").strip()
        time = parse_time_text(time, snapshot_date)
        title = doassert_single(item.xpath('.//h3[@class="headline"]/a/text()'), "link err").strip()
        content = doassert_or_empty_str(item.xpath('.//div[@class="summary-container"]/p/text()'), "link err").strip()
        ret.append(Document(time, title, content, clean_url_wsj(link)))
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
                ret.append(Document(time, title, content, clean_url_wsj(link)))
            except Exception as e:
                print(etree.tostring(item).decode("utf-8"))
                print(snapshot_date)
                raise e
        return ret
    raise RuntimeError("unknown page")
    
def parse_wsj_page_news_china_2015(tree, snapshot_date: datetime) -> List[Document]:
    # first = tree.xpath('//div[@class="firstArticle item-container"]')
    # doassert(len(first) == 1, "head err"+str(first))
    items = tree.xpath('//div[@class="newsContainer"]')
    items += tree.xpath('//div[@class="headlineSummary topStory storyType-noImage"]/ul/li/p/..')
    # doassert(len(items) >=1, "items err")
    ret = []
    time = snapshot_date
    for item in items:
        links = item.xpath('.//h1/a/@href')
        if len(links) == 1:
            link = links[0].strip()
        elif len(links) == 0:
            item = item.getparent()
            links = item.xpath('.//h1/a/@href')
            link = doassert_single(links, "link err")
        else:
            doassert(links.__len__() == 3, "link err")
            link = links[1].strip()
        title_segs = item.xpath('.//h1/a/text()')
        doassert(len(title_segs) >0, "title err " + str(title_segs))
        title = "".join(title_segs).strip()
        content = doassert_single(item.xpath('.//p/text()'), "link err").strip()
        ret.append(Document(time, title, content, clean_url_wsj(link)))
    items = tree.xpath('//li[@class=" subPrev tipTree tooltipType-news" or @class="firstList subPrev tipTree tooltipType-news"]/p')
    doassert(len(items) >=4, "items err")
    for item in items:
        content = item.text
        ii: etree._Element = item
        item = ii.getparent()
        title_segs = item.xpath('.//h2[@class="tipTarget"]/a/text()')
        doassert(len(title_segs) >0, "title err " + str(title_segs))
        title = "".join(title_segs).strip()
        links = item.xpath('.//h2[@class="tipTarget"]/a/@href')
        if len(links) == 2:
            links.pop()
        link = doassert_single(links, "link err").strip()
        ret.append(Document(time, title, content, clean_url_wsj(link)))
    return ret

badcount = 0
def parser_wsj_news_types_china_news(tree, snapshot_date: datetime) -> List[Document]:
    script = tree.xpath("//body/script/text()")
    for script_str in script:
        idx = script_str.find("window.__STATE__ = ")
        if idx == -1:
            continue
        jsondata = script_str[idx+len("window.__STATE__ = "): script_str.rfind(";")]
        ret = []
        try:
            data: dict = json.loads(jsondata)
            data = data['data']
            sb = 0
            for key, v in data.items():
                if key.startswith("article_SB") or key.startswith("article|capi_SB"):
                    sb += 1
                    link = v['data']['id']
                    core = v['data']['data']
                    if core['articleSection'] != "World":
                        continue
                    content = core['summary']
                    ts = core['timestamp']
                    time = datetime.fromtimestamp(ts/1000, ny_tz).astimezone(beijing)
                    title = core['headline']
                    ret.append(Document(time, title, content, link))
            doassert(len(ret) >= 2 or sb < 10, "num ar " + str(ret))
        except Exception as e:
            with open("out/err.json", 'w', encoding="utf-8") as f:
                f.write(jsondata)
            raise e
        return ret
    global badcount
    badcount += 1
    if snapshot_date.year >= 2021 and badcount < 10:
        print("skip bad page" + str(snapshot_date))
        return []
    return parse_wsj_news_world_china_2015(tree, snapshot_date)