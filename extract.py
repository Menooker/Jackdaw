from io import TextIOWrapper
import tarfile
from datetime import datetime, timedelta
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
from Jackdaw.Parser.wsj import parse_wsj_news_world_china_2015, parse_wsj_world_china_2023


class Buffer:
    def __init__(self, logf: TextIOWrapper, zipf: zipfile.ZipFile) -> None:
        self.docs: List[Document] = []
        self.logs: List[str] = []
        self.filename = ""
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
        self.buffer: Dict[str, str] = dict()
    def get(self, tarf: tarfile.TarFile, name: str) -> str:
        if name in self.buffer:
            ret = self.buffer[name]
            del self.buffer[name]
            return ret
        for _ in range(100):
            nxt = tarf.next()
            if not nxt:
                print("warn cannot find file "+ name)
                return
            with tarf.extractfile(nxt) as file:
                contents = file.read().decode("utf-8")
            if name == nxt.name:
                return contents
            self.buffer[nxt.name] = contents
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
    def enumerate_files(self, tgz_path: str, parser):
        with tarfile.open(tgz_path, 'r:xz') as f:
            members = f.getmembers()
            members = sorted(members, key = lambda m: m.name)
        with tarfile.open(tgz_path, 'r:xz') as f:
            rob = ReorderBuffer()
            # while next:
            for next in tqdm(members):
                fn = next.name
                contents = rob.get(f, fn)
                if fn in self.donefile:
                    continue
                if contents is None:
                    continue
                if "No recent results have been found." in contents:
                    # next = f.next()
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
    def work(self, numfiles: int, parser):
        for i in range(numfiles):
            ctx.enumerate_files(f"out/{self.name}_{i}.tar.xz", parser)
        ctx.buffer.close()
        logpath = f"out/{self.name}_extract_log.txt"
        with open(logpath, 'r', encoding="utf-8") as logf:
            logs = logf.read()
        with zipfile.ZipFile(f"out/{self.name}_extract.zip", 'a', compression=zipfile.ZIP_LZMA) as zipf:
            zipf.writestr("logs.txt", logs)


# ctx = Context("wsj-news-world-china")
# thees = ctx.work(1, parse_wsj_news_world_china_2015)
ctx = Context("wsj-world-china")
thees = ctx.work(1, parse_wsj_world_china_2023)