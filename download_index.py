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
import argparse

def html_has_encoding(text: str) -> bool:
    if text.startswith('<?xml version="1.0" encoding="'):
        return True
    if text.find('http-equiv="Content-Type" content="text/html; charset=', 0, 1000) != -1:
        return True
    return False


def request(url: str):
    err = None
    for i in range(5):
        try:
            r = requests.get(url, timeout=60)
            r.encoding = r.apparent_encoding
            if r.text.find("You have sent too many requests in a given amount of time.", 0, 200) != -1:
                print("Too many requests!!! Sleep 128 sec")
                time.sleep(128)
                continue
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


def get_main_pages_impl(url, start_timestamp, end_timestamp) -> List[str]:
    user_agent = "Mozilla/5.0 (Windows NT 5.1; rv:40.0) Gecko/20100101 Firefox/40.0"
    cdx = WaybackMachineCDXServerAPI(url, user_agent, start_timestamp=start_timestamp, end_timestamp=end_timestamp)
    urls = [item.archive_url for item in tqdm(cdx.snapshots()) if item.statuscode == "200"]
    return urls

def get_main_pages(name, url, start_timestamp, end_timestamp):
    cache_dir = f"out/{name}/mainpages.txt"
    if not os.path.exists(cache_dir):
        urls = get_main_pages_impl(url, start_timestamp, end_timestamp)
        with open(cache_dir, 'w') as f:
            f.write("\n".join(urls))
        return urls
    with open(cache_dir, 'r') as f:
        return f.read().split("\n")


class Context:
    def __init__(self, name, url, start_timestamp, end_timestamp):
        os.makedirs(f"out/{name}", exist_ok=True)
        self.exec_main = ThreadPoolExecutor(2)
        self.lock = RLock()
        self.mainurls = get_main_pages(name, url, start_timestamp, end_timestamp)
        self.next_url_idx = 0
        main_path = f"out/{name}/main.txt"
        self.done_main: Set[str] = set(file_to_list(main_path))
        self.done_main_log = open(main_path, 'a')
        self.main_fut: Set[Future] = set()
        self.outzip = zipfile.ZipFile(f"out/{name}.zip", 'a', compression=zipfile.ZIP_LZMA)
        self.ziplock = Lock()
        self.bad_content_count = 0
        self.name = name
        self.url = url

    def close(self):
        print("Calling shutdown")
        self.exec_main.shutdown(wait=False, cancel_futures=True)
        print("Waiting for workers, timeout 180...")
        def exitfunc():
            self.exec_main.shutdown(wait=True)
        wait_close = threading.Thread(target=exitfunc, daemon=True)
        wait_close.start()
        try:
            wait_close.join(timeout=180)
        except TimeoutError:
            print("Timeout")
            exit(2)
        finally:
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
    
    def _write_bad_page(self, url, content):
        print(f"Bad content and try to resume:{url} {content}")
        with self.lock:
            self.bad_content_count += 1
            with open(f"out/{self.name}/bad_content.txt", 'a', encoding="utf-8") as f:
                f.write(f">>>>>>>\n{url}\n{content}\n")
            if self.bad_content_count == 20:
                raise RuntimeError(f"Bad content:{url} {content}")

    def _task_main_page(self, url: str):
        try:
            r = request(url)
            try:
                tree = etree.HTML(r.text)
            except ValueError as e:
                if str(e) == "Unicode strings with encoding declaration are not supported. Please use bytes input or XML fragments without declaration.":
                    tree = etree.HTML(r.content)
                else:
                    raise e
            pageid = trim_main_url(url)
            if tree is None:
                with self.lock:
                    self._on_main_done(pageid)
            if "The Wayback Machine is an" in r.text and "response at crawl time" in r.text:
                pass
            else:
                path = f"{pageid}.html"
                with self.ziplock:
                    self.outzip.writestr(path, r.text)
                    self.outzip.fp.flush()
            with self.lock:
                self._on_main_done(pageid)
            time.sleep(7)
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

def main(name, url, start_timestamp, end_timestamp):
    ctx = Context(name, url, start_timestamp, end_timestamp)
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

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--url", type=str, required=True)
    parser.add_argument("--name", type=str, required=True)
    parser.add_argument("--start", type=int, required=True)
    parser.add_argument("--end", type=int, required=True)

    args = parser.parse_args()
    main(args.name, args.url, args.start, args.end)
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