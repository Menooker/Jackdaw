from concurrent import futures
import threading
from typing import Dict, List, Literal, Set
import lxml
from waybackpy import WaybackMachineCDXServerAPI
from tqdm import tqdm
import requests
from lxml import etree
import os
from concurrent.futures import ThreadPoolExecutor, Future
from threading import RLock, Lock
from gne import GeneralNewsExtractor
import traceback
from datetime import datetime
import time
import signal
import zipfile
from Jackdaw.Utils import get_real_url, trim_article_url, trim_main_url, all_not_in
from Jackdaw.Parser import *

proxies = { 
              "http"  : "http://127.0.0.1:8010", 
              "https" : "http://127.0.0.1:8010",
            }

def html_has_encoding(text: str) -> bool:
    if text.startswith('<?xml version="1.0" encoding="'):
        return True
    if text.find('http-equiv="Content-Type" content="text/html; charset=', 0, 1000) != -1:
        return True
    return False


def request(url: str, proxy: Dict[str, str]):
    err = None
    for i in range(5):
        try:
            r = requests.get(url, proxies=proxy, timeout=60)
            r.encoding = r.apparent_encoding
            if r.encoding == 'utf-8' or html_has_encoding(r.text):
                return r
            if r.text.find('<meta charset="utf-8"', 0, 1000) != -1:
                r.encoding = 'utf-8'
                return r
            r.encoding = 'gb2312'
            return r
        except Exception as e:
            print(f"request error for url {url}: {str(e)}, sleep {2**(i + 6)}")
            err = e
            time.sleep(2**(i + 6))
    raise err

def file_to_list(cache_dir):
    if  os.path.exists(cache_dir):
        with open(cache_dir, 'r') as f:
            return f.read().split("\n")
    return []


def get_main_pages_impl() -> List[str]:
    url = "https://finance.sina.com.cn/"
    user_agent = "Mozilla/5.0 (Windows NT 5.1; rv:40.0) Gecko/20100101 Firefox/40.0"
    cdx = WaybackMachineCDXServerAPI(url, user_agent, start_timestamp=2012, end_timestamp=2025)
    urls = [item.archive_url for item in tqdm(cdx.snapshots())]
    return urls

def get_main_pages():
    cache_dir = "out/sina/mainpages.txt"
    if not os.path.exists(cache_dir):
        urls = get_main_pages_impl()
        with open(cache_dir, 'w') as f:
            f.write("\n".join(urls))
        return urls
    with open(cache_dir, 'r') as f:
        return f.read().split("\n")


def fetch_page_or_fallback(url, fn):
    r = request(get_real_url(url), {})
    isempty = "<body></body>" in r.text or not r.text
    if "<body" not in r.text or isempty:
        anchor = '<META HTTP-EQUIV="Refresh" CONTENT="0;URL='
        idx = r.text.find(anchor)
        if idx != -1:
            idx2 = r.text.find('">', idx)
            redo_url = r.text[idx+len(anchor): idx2]
            print("Redirect to:", redo_url)
            r = request(redo_url, {})
        elif "<TITLE> 页面没有找到 " in r.text or isempty:
            r = request(url, proxies)
        else:
            with open(f"out/err_{fn}.html", 'w', encoding="utf-8") as f:
                f.write(r.text)
            ctx._write_bad_page(url, f"Unknown page out/err_{fn}.html")
            return None
    return r

class MainPage:
    def __init__(self, url: str, num_news: int):
        self.num_news = num_news
        self.id = url

class Context:
    def __init__(self):
        os.makedirs("out/sina", exist_ok=True)
        self.exec_main = ThreadPoolExecutor(2)
        self.exec_work = ThreadPoolExecutor(4)
        self.lock = RLock()
        self.mainurls = get_main_pages()
        self.next_url_idx = 0
        main_path = "out/sina/main.txt"
        article_path = "out/sina/article.txt"
        self.done_main: Set[str] = set(file_to_list(main_path))
        self.done_article: Set[str] = set(file_to_list(article_path))
        self.pending_article: Dict[str, Set[MainPage]] = dict()
        self.done_main_log = open(main_path, 'a')
        self.done_article_log = open(article_path, 'a')
        self.main_fut: Set[Future] = set()
        self.outzip = zipfile.ZipFile("out/sina.zip", 'a', compression=zipfile.ZIP_LZMA)
        self.ziplock = Lock()
        self.allowed_short = file_to_list("out/sina/allow_short.txt")
        self.bad_content_count = 0
        self.parsers = [Parser2013_v1, Parser2012_v3, Parser2012_v1, Parser2012_v2]
        with open("out/sina/bad_content.txt", 'a') as f:
            f.write("=======\n")

    def close(self):
        print("Calling shutdown")
        self.exec_main.shutdown(wait=False, cancel_futures=True)
        self.exec_work.shutdown(wait=False, cancel_futures=True)
        print("Waiting for workers, timeout 180...")
        def exitfunc():
            self.exec_work.shutdown(wait=True)
            self.exec_main.shutdown(wait=True)
        wait_close = threading.Thread(target=exitfunc, daemon=True)
        wait_close.start()
        try:
            wait_close.join(timeout=180)
        except TimeoutError:
            print("Timeout")
            exit(2)
        finally:
            self.done_article_log.close()
            self.done_main_log.close()
            self.outzip.close()
        print("shutdown done")
    
    def _on_main_done(self, pid):
        self.done_main_log.write(pid+"\n")
        self.done_main.add(pid)
        num_done = len(self.done_main)
        num_all = len(self.mainurls)
        print(f"[{num_done}/{num_all}={num_done/num_all*100:.2f}]")
        self.submit_fetch_main()

    def _on_article_done(self, trimed_url: str, cur_main: MainPage):
        if trimed_url not in self.done_article:
            self.done_article_log.write(trimed_url+"\n")
            self.done_article_log.flush()
            self.done_article.add(trimed_url)
        if trimed_url in self.pending_article:
            mains = self.pending_article[trimed_url]
            del self.pending_article[trimed_url]
        else:
            mains = [cur_main]
        for main in mains:
            main.num_news -= 1
            if main.num_news == 0:
                self._on_main_done(main.id)
        self.done_main_log.flush()
    
    def _write_bad_page(self, url, content):
        print(f"Bad content and try to resume:{url} {content}")
        with self.lock:
            self.bad_content_count += 1
            with open("out/sina/bad_content.txt", 'a', encoding="utf-8") as f:
                f.write(f">>>>>>>\n{url}\n{content}\n")
            if self.bad_content_count == 20:
                raise RuntimeError(f"Bad content:{url} {content}")


    def submit_fetch_article(self, url: str, main: MainPage):
        trimed = trim_article_url(url)
        with self.lock:
            if trimed in self.done_article:
                print("Already fetched page:", url)
                # already fetched
                self._on_article_done(trimed, main)
                return
            if trimed in self.pending_article:
                # already fetching
                # print("Already fetching page:", url)
                self.pending_article[trimed].add(main)
                return
            else:
                # we are the first to fetch
                self.pending_article[trimed] = {main}
        def fetch():
            try:
                trimed = trim_article_url(url)
                fn = trimed.replace(":", "-.").replace("/", "--")
                r = fetch_page_or_fallback(url, fn)
                if r is None:
                    time.sleep(2)
                    return "Resume"
                extractor = GeneralNewsExtractor()
                try:
                    result = extractor.extract(r.text, noise_node_list=['//div[@class="blkContentFooter"]',
                        '//div[@class="blkComment otherContent_01"]',
                        '//div[@class="corrTxt_01"]',
                        '//a[@href="http://news.sina.com.cn/437/2008/0703/30.html"]',
                        '//a[@href="http://guba.sina.com.cn"]',
                        '//a[text()="微博推荐"]',
                        '//table[@class="tb01"]/tbody',
                        '//div[@id="title_gi"]',
                        '//dl[@id="dl_gi"]',
                        '//div[@class="content_aboutNews clearfix"]',
                        '//div[@class="page-footer"]',
                        '//div[@class="ad_content ad_06 adNone"]',
                        '//div[@class="article-keywords"]',
                        '//div[@class="article-feedback"]',
                        '//div[@class="feed-wrap"]',
                        '//div[@class="sina-comment-wrap"]',
                        '//div[@class="page-tools"]',
                        '//div[@class="zhitou-wrap"]',
                        '//div[@class="artical-player-wrap"]',
                        '//div[@class="img_wrapper"]',
                        '//div[@class="xb_new_finance_app"]',
                        '//span[starts-with(@id,"quote_")]',
                        '//table',
                        '//div[@class="finance_app_zqtg finance_lcsds_ds_cls"]',
                        '//div[@class="seo_data_list"]',
                        '//div[@class="footer-wrap clearfix"]',
                        '//div[@class="ct_hqimg"]',
                        '//div[@data-sudaclick="ad_content_top"]',
                        ])
                    tree = etree.HTML(r.text)
                    table_text = ""
                    table = tree.xpath('//table')
                    if table:
                        tables = [" ".join(telem.xpath('tbody/tr/td//text()')) for telem in table]
                        table_text = "\n"
                        table_text += "\n".join(tables)
                    is_video = tree.xpath('//div[@class="artical-player-wrap"]').__len__() >= 1
                except Exception as e:
                    if trimed in self.allowed_short or str(e) == "Document is empty":
                        with self.lock:
                            self._on_article_done(trimed, main)
                        time.sleep(2)
                        return url
                    with open(f"out/err_{fn}.html", 'w', encoding="utf-8") as f:
                        f.write(r.text)
                    ctx._write_bad_page(url, f"{str(e)} Unknown page out/err_{fn}.html")
                    time.sleep(2)
                    return "Resume"
                # patch the data
                result["content"] = result["content"].replace("[微博]", "")
                if table_text:
                    result["content"] += "\n" + table_text
                not_found_str = ["页面没有找到 5秒钟之后将会带", "Wayback Machine"]
                must_be_ok = ["中国人民银行公开市场业务操作室", "接受组织调查", "正回购",
                    "省纪委", "逆回购", "今日未进行公开市场操作", "新华社快讯", "新华社讯",
                    "责任编辑：张玉洁 SF107", "孙剑嵩", "刘万里 SF014", # 这编辑喜欢发纯图片
                    "一图", "一张图"
                    ]
                must_be_ok_title = ["一图", "一张图"] # 纯图文
                if all_not_in(not_found_str, result["content"]) and "页面没有找到" != result["title"]:
                    if sum([result["content"].count(x) for x in "，。%；、："]) < 3 \
                        and trimed not in self.allowed_short \
                        and all_not_in(must_be_ok, result["content"]) \
                        and all_not_in(must_be_ok_title, result["title"]) \
                        and not is_video:
                            self._write_bad_page(url, result['content'])
                            time.sleep(2)
                            return "Resume"
                    timestr = result["publish_time"]
                    # parse ISO timestamp '2014-10-01T17:19:43+08:00'
                    if "+" in timestr:
                        timestr = timestr[:timestr.find("+")]
                    date_fmt = ["%Y年%m月%d日 %H:%M", "%Y-%m-%d %H:%M",
                        "%Y-%m-%d %H:%M:%S", "%Y年%m月%d日 %H:%M:%S", '%Y-%m-%dT%H:%M:%S']
                    err = None
                    for fmt in date_fmt:
                        try:
                            date = datetime.strptime(timestr, fmt)
                            break
                        except ValueError as e:
                            err = e
                    else:
                        self._write_bad_page(url, str(err))
                        time.sleep(2)
                        return "Resume"
                    data = "[sepsep]".join([result["title"], result["author"], date.strftime("%Y/%m/%d %H:%M"), result["content"]])
                    base = f"{date.year}/{date.month}/{date.day}/{date.hour:02d}/{date.minute:02d}"
                    # os.makedirs(base, exist_ok=True)
                    path = f"{base}/{fn}.txt"
                    with self.ziplock:
                        self.outzip.writestr(path, data)
                        self.outzip.fp.flush()
                    # with open(f"{base}/{fn}.txt", 'w', encoding="utf-8") as f:
                    #     f.write(data)
                    #     print("file:", f"{base}/{fn}.txt")
                else:
                    print(url, "page not exists")
                with self.lock:
                    self._on_article_done(trimed, main)
                time.sleep(2)
                return url
            except:
                with self.lock:
                    del self.pending_article[trimed]
                print("Error on url:", url)
                traceback.print_exc()
                return None

        fut = self.exec_work.submit(fetch)
        with self.lock:
            self.main_fut.add(fut)

    def _task_main_page(self, url: str):
        try:
            r = request(url, proxies)
            try:
                tree = etree.HTML(r.text)
            except ValueError as e:
                if str(e) == "Unicode strings with encoding declaration are not supported. Please use bytes input or XML fragments without declaration.":
                    tree = etree.HTML(r.content)
                else:
                    raise e
            
            for parser in self.parsers:
                parse_cmd = parser.is_it(url, tree, r.text)
                if parse_cmd == "good":
                    urls = parser.parse(url, tree)
                    page = MainPage(trim_main_url(url), len(urls))
                    print("num pages:", len(urls), "from", url)
                    for vurl in urls:
                        self.submit_fetch_article(vurl, page)
                    break
                elif parse_cmd == "skip":
                    with self.lock:
                        self._on_main_done(trim_main_url(url))
                    break
            else:
                print(f"Bad main and try to resume:{url}")
                self._write_bad_page(url, "main")
                time.sleep(10)
                return "Resume"
            time.sleep(10)
            return url
        except:
            traceback.print_exc()
            return None
    
    def submit_fetch_main(self):
        with self.lock:
            while self.next_url_idx < len(self.mainurls):
                cur = self.next_url_idx
                self.next_url_idx += 1
                url = trim_main_url(self.mainurls[cur])
                if url in self.done_main:
                    # print("main already fetched:", url)
                    continue
                self.main_fut.add(self.exec_main.submit(self._task_main_page, self.mainurls[cur]))


ctx = Context()
def handler(signum, frame):
    print("ctrl+c")
    ctx.close()
    print("Num bad contents", ctx.bad_content_count)
    exit(2)

signal.signal(signal.SIGINT, handler)
for _ in range(1):
    for _ in range(16):
        ctx.submit_fetch_main()
    while True:
        with ctx.lock:
            futs = list(ctx.main_fut)
        if len(futs) == 0:
            print("Done")
            break
        try:
            results = futures.wait(futs, return_when="FIRST_COMPLETED", timeout=5)
        except TimeoutError:
            continue
        for fut in results.done:
            if fut.done():
                ret = fut.result()
                print("index", ret)
            else:
                ret = None
            if ret is None:
                ctx.close()
                print("Error occured, exiting")
                exit(2)
            with ctx.lock:
                ctx.main_fut.remove(fut)

print("Num bad contents", ctx.bad_content_count)
ctx.close()
    # def parse_2012(self, url: str, tree):
    #     news_src_url = tree.xpath('//ul[@id="S_Cont_06_01"]/script[@type="text/javascript"]/@src')
    #     if news_src_url.__len__() != 1:
    #         raise RuntimeError("parse 2012 failed: "+ url)

    #     r = requests.get(news_src_url[0])
    #     start = "var all_1_data ="
    #     end = ";"
    #     sidx = r.text.find(start)
    #     if sidx == -1:
    #         raise RuntimeError("parse 2012 JS: "+ news_src_url[0])
    #     eidx = r.text.find(end, sidx)
    #     if eidx == -1:
    #         raise RuntimeError("parse 2012 JS: "+ news_src_url[0])
    #     data = json.loads(r.text[sidx + len(start): eidx])
    #     inner = data['data']
    #     length = len(inner)

    #     news_src_url = tree.xpath('//ul[@id="S_Cont_06_02"]/li/a/href')

    #     page = MainPage(trim_main_url(url, length))
    #     for obj in inner:
    #         self.submit_fetch_article(obj['url'], page)
    
# def main():
    

# url = 'https://web.archive.org/web/20120101175409/http://finance.sina.com.cn/'
# # url = 'https://web.archive.org/web/20120205053606/http://finance.sina.com.cn/roll/20120202/044211294946.shtml'
# r = requests.get(url)
# r.encoding = 'gb2312'

# tree = etree.HTML(r.text)
# print(11111111111)


# with open("test2.html", 'w', encoding='utf-8') as f:
#     f.write(r.text)


'''//div[@class="blkContentFooter"]
//div[@class="blkComment otherContent_01"]
//div[@class="corrTxt_01"]
'''