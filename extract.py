from io import TextIOWrapper
import tarfile
from datetime import datetime, timedelta
from unicodedata import bidirectional
import pytz
from lxml import etree
from dataclasses import dataclass
from typing import Dict, List, Set
from zoneinfo import ZoneInfo
import os
import zipfile
from tqdm import tqdm
from Jackdaw.Parser.Utils import Document, doassert, doassert_or_empty_str, doassert_single, datetime_to_timestamp, timestamp_to_datetime, ny_to_shanghai_time
from Jackdaw.Parser.Utils import beijing, gmt_timestamp_to_shanghai_time
from Jackdaw.Parser.wsj import parse_wsj_news_world_china_2015, parse_wsj_world_china_2023, \
    parse_wsj_page_news_china_2015,parser_wsj_news_types_china_news
from Jackdaw.Parser.bloomberg import parse_bloomberg_topics_china_2015, parse_bloomberg_news_china_2012, parse_bloomberg_next_china_2019


class Buffer:
    def __init__(self, logf: TextIOWrapper, zipf: zipfile.ZipFile) -> None:
        self.docs: List[Document] = []
        self.logs: List[str] = []
        self.filename = "init.txt"
        self.logf = logf
        self.zipf = zipf

    def flush(self):
        if self.logs.__len__()==0:
            return
        file = "==========\n".join([f"{doc.title}!!!!!!!!!\n{datetime_to_timestamp(doc.timestamp)}!!!!!!!!!\n{doc.url}!!!!!!!!!\n{doc.content}" for doc in self.docs])
        self.zipf.writestr(self.filename, file + "\n")
        self.zipf.fp.flush()
        self.logs.append("")
        self.logf.write("\n".join(self.logs))
        self.logf.flush()

        self.logs.clear()
        self.docs.clear()
        self.filename = ""
    
    def close(self):
        self.flush()
        self.logf.close()
        self.zipf.close()

    def push(self, doc: Document):
        if self.docs.__len__() == 0:
            self.filename = f"{datetime_to_timestamp(doc.timestamp)}.txt"
        self.logs.append(f"{self.filename} {len(self.docs)} {doc.url} {datetime_to_timestamp(doc.timestamp)}")
        self.docs.append(doc)
        if self.docs.__len__() >= 200:
            self.flush()
    
    def push_index(self, fn: str):
        self.logs.append(f"done@{fn}")
        
class ReorderBuffer:
    def __init__(self) -> None:
        self.buffer: Dict[str, List[str]] = dict()
    def get(self, tarf: tarfile.TarFile, name: str) -> str:
        if name in self.buffer:
            retlist = self.buffer[name]
            ret = retlist.pop()
            if len(retlist) == 0:
                del self.buffer[name]
            return ret
        for _ in range(200):
            try:
                nxt = tarf.next()
            except EOFError:
                print("EOF error")
                nxt = None
            if not nxt:
                print("warn cannot find file "+ name)
                return
            try:
                with tarf.extractfile(nxt) as file:
                    contents = file.read().decode("utf-8")
            except EOFError:
                print("EOF error")
                return
            if name == nxt.name:
                return contents
            self.buffer[nxt.name] = [contents]
        else:
            print("warn cannot find file "+ name)

snapshot_example = "20160119083341"
bad_index= {"20160120134935.html"}

class Context:
    def __init__(self, name: str) -> None:
        self.name = name
        self.met: Dict[str, datetime] = dict()
        self.donefile: Set[str] = set()
        logpath = f"out/{name}_extract_log.txt"
        if os.path.exists(logpath):
            with open(logpath, 'r', encoding="utf-8") as f:
                for line in f.readlines():
                    line = line.strip()
                    if line.startswith("done@"):
                        self.donefile.add(line[len("done@"):])
                        continue
                    if not line:
                        continue
                    spl = line.split(" ")
                    doassert(len(spl) == 4, "bad log line "+ line)
                    self.met[spl[2]] = timestamp_to_datetime(spl[3]).replace(tzinfo=beijing)
        logf = open(logpath, 'a', encoding="utf-8")
        zipf = zipfile.ZipFile(f"out/{name}_extract.zip", 'a', compression=zipfile.ZIP_LZMA)
        self.buffer = Buffer(logf, zipf)
    def enumerate_files(self, tgz_path: str, parser, badids: List[str]):
        filename_cache_path = tgz_path + ".list.txt"
        if os.path.exists(filename_cache_path):
            with open(filename_cache_path, "r", encoding="utf-8") as f:
                members = [line.strip() for line in f.readlines()]
        else:
            with tarfile.open(tgz_path, 'r:xz') as f:
                members = []
                while True:
                    try:
                        nxt = f.next()
                    except EOFError:
                        nxt = None
                        print("EOF error")
                    if nxt:
                        members.append(nxt.name)
                    else:
                        break
                members = sorted(members)
            with open(filename_cache_path, "w", encoding="utf-8") as f:
                f.write("\n".join(members))
        with tarfile.open(tgz_path, 'r:xz') as f:
            rob = ReorderBuffer()
            # while next:
            for next in tqdm(members):
                fn = next
                contents = rob.get(f, fn)
                if fn in self.donefile:
                    continue
                if contents is None:
                    continue
                if "No recent results have been found." in contents:
                    # next = f.next()
                    continue
                if contents.find("503 Service Unavailable", 0, 1000) != -1:
                    continue
                if contents.find("<title>Wayback Machine</title>", 0, 500) != -1:
                    continue
                if contents.find("We've detected unusual activity from your computer network", 2000, 20000) != -1:
                    continue
                if any(map(lambda x: contents.find(x, 0, 1000) != -1,  badids)):
                    continue
                try:
                    snapshot_date = gmt_timestamp_to_shanghai_time(fn[:len(snapshot_example)])
                    tree = etree.HTML(contents)
                    results = parser(tree, snapshot_date)
                    for r in results:
                        if r.url not in self.met:
                            self.buffer.push(r)
                            self.met[r.url] = r.timestamp
                        else:
                            ts = self.met[r.url]
                            if ts > r.timestamp + timedelta(hours=24):
                                # print(f"Warn: cannot update TS {ts} vs {r}")
                                pass
                    self.buffer.push_index(fn)
                    # for r in results:
                    #     print(r)
                except RuntimeError as e:
                    with open("out/err.html", 'w', encoding="utf-8") as outf:
                        outf.write(contents)
                    raise e
    def work(self, numfiles: int, parser, startfile: int = 0, badids: List[str] = []):
        for i in range(startfile, numfiles):
            ctx.enumerate_files(f"out/{self.name}_{i}.tar.xz", parser, badids)
        ctx.buffer.close()
        logpath = f"out/{self.name}_extract_log.txt"
        with open(logpath, 'r', encoding="utf-8") as logf:
            logs = logf.read()
        with zipfile.ZipFile(f"out/{self.name}_extract.zip", 'a', compression=zipfile.ZIP_LZMA) as zipf:
            zipf.writestr("logs.txt", logs)


# ctx = Context("wsj-news-world-china")
# thees = ctx.work(1, parse_wsj_news_world_china_2015)
# ctx = Context("wsj-world-china")
# thees = ctx.work(1, parse_wsj_world_china_2023)
# ctx = Context("wsj-page-news-china")
# thees = ctx.work(1, parse_wsj_page_news_china_2015)
# ctx = Context("wsj-news-types-china-news")
# thees = ctx.work(3, parser_wsj_news_types_china_news, 0, badids=["20180131024848", "20180202084856", "20190207080113", "20190711050951"])
# ctx = Context("bloomberg-topics-china")
# thees = ctx.work(1, parse_bloomberg_topics_china_2015)
# ctx = Context("bloomberg-news-china")
# thees = ctx.work(1, parse_bloomberg_news_china_2012)
# ctx = Context("bloomberg-next-china")
# thees = ctx.work(1, parse_bloomberg_next_china_2019)
ctx = Context("wsj-public-page-news-china2")
thees = ctx.work(1, parse_wsj_page_news_china_2015)