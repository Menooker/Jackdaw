from datetime import timedelta, datetime
from zoneinfo import ZoneInfo
from dataclasses import dataclass
def doassert(b, err):
    if not b:
        raise RuntimeError(err)

def doassert_single(b, err):
    doassert(len(b) == 1, err + " " + str(b))
    return b[0]

def doassert_or_empty_str(b, err):
    if len(b) == 0:
        return ""
    doassert(len(b) == 1, err + " " + str(b))
    return b[0]

def datetime_to_timestamp(d: datetime) -> str:
    return d.strftime("%Y%m%d%H%M")

    
def timestamp_to_datetime(time_str:str) -> datetime:
    return datetime.strptime(time_str, "%Y%m%d%H%M%S")


ny_tz = ZoneInfo('America/New_York')
beijing = ZoneInfo("Asia/Shanghai")
gmt = ZoneInfo("GMT")



def gmt_timestamp_to_shanghai_time(time_str:str) -> datetime:
    gmt_time = timestamp_to_datetime(time_str)
    gmt_time = gmt_time.replace(tzinfo=gmt)
    return gmt_time.astimezone(beijing)

def ny_to_shanghai_time(t: datetime) -> datetime:
    return t.replace(tzinfo=ny_tz).astimezone(beijing)



@dataclass
class Document:
    timestamp: datetime
    title: str
    content: str
    url: str

