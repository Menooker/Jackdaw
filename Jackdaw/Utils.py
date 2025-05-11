from typing import List
base_url_prefix = 'https://web.archive.org/web/'


def trim_main_url(url: str) -> str:
    '''
    'https://web.archive.org/web/20120101175409/http://finance.sina.com.cn/'
    '''
    idx = url.find("/http:", len(base_url_prefix))
    return url[len(base_url_prefix):idx]

def trim_article_url(url: str) -> str:
    '''
    'https://web.archive.org/web/20120205053606/http://finance.sina.com.cn/roll/20120202/044211294946.shtml'
    '''
    assert(url.startswith(base_url_prefix))
    return url[len(base_url_prefix)+len("20120205053606/http://"):]

def get_real_url(url: str) -> str:
    idx = url.find("http", len(base_url_prefix))
    return url[idx:]

def do_assert(url, v):
    if not v:
        raise RuntimeError("failed at "+ url)

def all_not_in(substr: List[str], s: str) -> bool:
    return all(map(lambda x: x not in s, substr))