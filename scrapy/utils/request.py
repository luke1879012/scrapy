"""
This module provides some useful functions for working with
scrapy.http.Request objects
"""

import hashlib
from typing import Dict, Iterable, Optional, Tuple, Union
from urllib.parse import urlunparse
from weakref import WeakKeyDictionary

from w3lib.http import basic_auth_header
from w3lib.url import canonicalize_url

from scrapy.http import Request
from scrapy.utils.httpobj import urlparse_cached
from scrapy.utils.python import to_bytes, to_unicode


_fingerprint_cache: "WeakKeyDictionary[Request, Dict[Tuple[Optional[Tuple[bytes, ...]], bool], str]]"
_fingerprint_cache = WeakKeyDictionary()


def request_fingerprint(
    request: Request,
    include_headers: Optional[Iterable[Union[bytes, str]]] = None,
    keep_fragments: bool = False,
) -> str:
    """
    Return the request fingerprint.
    返回请求指纹。

    The request fingerprint is a hash that uniquely identifies the resource the
    request points to. For example, take the following two urls:
    请求指纹是一个哈希，用于唯一标识请求指向的资源。例如，采用以下两个URL：

    http://www.example.com/query?id=111&cat=222
    http://www.example.com/query?cat=222&id=111

    Even though those are two different URLs both point to the same resource
    and are equivalent (i.e. they should return the same response).
    即使这些是两个不同的URL都指向相同的资源并且是等效的（即它们应该返回相同的响应）。

    Another example are cookies used to store session ids. Suppose the
    following page is only accessible to authenticated users:
    另一个示例是用于存储会话ID的Cookie。假设只有经过身份验证的用户才能访问以下页面：

    http://www.example.com/members/offers.html

    Lot of sites use a cookie to store the session id, which adds a random
    component to the HTTP Request and thus should be ignored when calculating
    the fingerprint.
    许多站点使用cookie来存储 session id，这会在HTTP请求中添加一个随机组件，因此在计算指纹时应将其忽略。

    For this reason, request headers are ignored by default when calculating
    the fingeprint. If you want to include specific headers use the
    include_headers argument, which is a list of Request headers to include.
    因此，在计算指纹时，默认情况下将忽略请求标头。如果要包括特定的标头，请使用include_headers参数，该参数是要包含的请求标头的列表。

    Also, servers usually ignore fragments in urls when handling requests,
    so they are also ignored by default when calculating the fingerprint.
    If you want to include them, set the keep_fragments argument to True
    (for instance when handling requests with a headless browser).
    此外，服务器在处理请求时通常会忽略url中的片段，因此在计算指纹时默认情况下也会忽略它们。
    如果要包括它们，请将keep_fragments参数设置为True（例如，在使用无头浏览器处理请求时）。

    """
    headers: Optional[Tuple[bytes, ...]] = None
    if include_headers:
        headers = tuple(to_bytes(h.lower()) for h in sorted(include_headers))
    cache = _fingerprint_cache.setdefault(request, {})
    cache_key = (headers, keep_fragments)
    if cache_key not in cache:
        fp = hashlib.sha1()
        fp.update(to_bytes(request.method))
        fp.update(to_bytes(canonicalize_url(request.url, keep_fragments=keep_fragments)))
        fp.update(request.body or b'')
        if headers:
            for hdr in headers:
                if hdr in request.headers:
                    fp.update(hdr)
                    for v in request.headers.getlist(hdr):
                        fp.update(v)
        cache[cache_key] = fp.hexdigest()
    return cache[cache_key]


def request_authenticate(request: Request, username: str, password: str) -> None:
    """Autenticate the given request (in place) using the HTTP basic access
    authentication mechanism (RFC 2617) and the given username and password
    """
    request.headers['Authorization'] = basic_auth_header(username, password)


def request_httprepr(request: Request) -> bytes:
    """Return the raw HTTP representation (as bytes) of the given request.
    This is provided only for reference since it's not the actual stream of
    bytes that will be send when performing the request (that's controlled
    by Twisted).
    """
    parsed = urlparse_cached(request)
    path = urlunparse(('', '', parsed.path or '/', parsed.params, parsed.query, ''))
    s = to_bytes(request.method) + b" " + to_bytes(path) + b" HTTP/1.1\r\n"
    s += b"Host: " + to_bytes(parsed.hostname or b'') + b"\r\n"
    if request.headers:
        s += request.headers.to_string() + b"\r\n"
    s += b"\r\n"
    s += request.body
    return s


def referer_str(request: Request) -> Optional[str]:
    """ Return Referer HTTP header suitable for logging. """
    referrer = request.headers.get('Referer')
    if referrer is None:
        return referrer
    return to_unicode(referrer, errors='replace')
