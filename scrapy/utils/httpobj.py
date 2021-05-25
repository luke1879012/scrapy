"""Helper functions for scrapy.http objects (Request, Response)"""

from typing import Union
from urllib.parse import urlparse, ParseResult
from weakref import WeakKeyDictionary

from scrapy.http import Request, Response

# 弱引用字典做个缓存
_urlparse_cache: "WeakKeyDictionary[Union[Request, Response], ParseResult]" = WeakKeyDictionary()


def urlparse_cached(request_or_response: Union[Request, Response]) -> ParseResult:
    """Return urlparse.urlparse caching the result, where the argument can be a
    Request or Response object
    返回urlparse.urlparse将结果缓存，其中参数可以是Request或Response对象
    """
    if request_or_response not in _urlparse_cache:
        # 如果不在这个字典中，则生成ParseResult对象
        _urlparse_cache[request_or_response] = urlparse(request_or_response.url)
    return _urlparse_cache[request_or_response]
