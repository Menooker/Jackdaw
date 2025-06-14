import zipfile
import os
from typing import List
from dataclasses import dataclass

@dataclass
class Record:
    ts: int
    zipfile: str
    file: str
    index: int

def get_index(path: str) -> List[Record]:
    ret = []
    for fn in os.listdir(path):
        if fn.endswith("extract.zip"):
            with zipfile.ZipFile(os.path.join(path, fn), 'r') as f:
                with f.open("logs.txt") as logf:
                    for line in logf.read().decode("utf-8").split("\n"):
                        if line.startswith("done@") or len(line) == 0:
                            continue
                        line = line.strip()
                        spl = line.split(" ")
                        if len(spl) != 4:
                            raise RuntimeError("Bad line " + line)
                        ret.append(Record(int(spl[3]), fn, spl[0], int(spl[1])))
    return ret

def get_index_sina(path: str) -> List[Record]:
    ret = []
    zipname = "sina.2025.5.zip"
    with zipfile.ZipFile(os.path.join(path, "sina.2025.5.zip"), 'r') as f:
        nlist = f.namelist()
    for fn in nlist:
        if fn.endswith("/"):
            continue
        spl = fn.split("/")
        if len(spl) != 6:
            raise RuntimeError("Bad line "+ fn)
        y, m, d, h, minu, fi = spl
        y, m, d, h, minu = map(int, (y, m, d, h, minu))
        ts = int(f"{y:04d}{m:02d}{d:02d}{h:02d}{minu:02d}")
        ret.append(Record(ts, zipname, fn, 0))
    return ret
def main(path: str):
    print("get compact")
    raw = get_index(path)
    print("get sina")
    raw.extend(get_index_sina(path))
    print("sort")
    raw.sort(key=lambda x: x.ts)
    print(len(raw))
    with zipfile.ZipFile(os.path.join(path, "index.zip"), 'w', compression=zipfile.ZIP_LZMA) as f:
        f.writestr("data.txt", "\n".join(map(lambda r: f"{r.ts} {r.zipfile} {r.file} {r.index}", raw)))

main("out/extracted")