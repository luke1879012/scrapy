import logging
import os
from typing import Optional, Set, Type, TypeVar

from twisted.internet.defer import Deferred

from scrapy.http.request import Request
from scrapy.settings import BaseSettings
from scrapy.spiders import Spider
from scrapy.utils.job import job_dir
from scrapy.utils.request import referer_str, request_fingerprint


BaseDupeFilterTV = TypeVar("BaseDupeFilterTV", bound="BaseDupeFilter")


class BaseDupeFilter:
    @classmethod
    def from_settings(cls: Type[BaseDupeFilterTV], settings: BaseSettings) -> BaseDupeFilterTV:
        return cls()

    def request_seen(self, request: Request) -> bool:
        return False

    def open(self) -> Optional[Deferred]:
        pass

    def close(self, reason: str) -> Optional[Deferred]:
        pass

    def log(self, request: Request, spider: Spider) -> None:
        """Log that a request has been filtered"""
        pass


RFPDupeFilterTV = TypeVar("RFPDupeFilterTV", bound="RFPDupeFilter")


class RFPDupeFilter(BaseDupeFilter):
    """Request Fingerprint duplicates filter"""

    def __init__(self, path: Optional[str] = None, debug: bool = False) -> None:
        self.file = None
        self.fingerprints: Set[str] = set()
        self.logdupes = True
        self.debug = debug
        self.logger = logging.getLogger(__name__)
        if path:
            self.file = open(os.path.join(path, 'requests.seen'), 'a+')
            self.file.seek(0)
            self.fingerprints.update(x.rstrip() for x in self.file)

    @classmethod
    def from_settings(cls: Type[RFPDupeFilterTV], settings: BaseSettings) -> RFPDupeFilterTV:
        debug = settings.getbool('DUPEFILTER_DEBUG')
        return cls(job_dir(settings), debug)

    def request_seen(self, request: Request) -> bool:
        # 生成指纹
        fp = self.request_fingerprint(request)
        # 判断是否在这个集合中
        if fp in self.fingerprints:
            return True
        # 不在则加入
        self.fingerprints.add(fp)
        if self.file:
            self.file.write(fp + '\n')
        return False

    def request_fingerprint(self, request: Request) -> str:
        return request_fingerprint(request)

    def close(self, reason: str) -> None:
        if self.file:
            self.file.close()

    def log(self, request: Request, spider: Spider) -> None:
        if self.debug:
            msg = "Filtered duplicate request: %(request)s (referer: %(referer)s)"
            args = {'request': request, 'referer': referer_str(request)}
            self.logger.debug(msg, args, extra={'spider': spider})
        elif self.logdupes:
            msg = ("Filtered duplicate request: %(request)s"
                   " - no more duplicates will be shown"
                   " (see DUPEFILTER_DEBUG to show all duplicates)")
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
            self.logdupes = False

        spider.crawler.stats.inc_value('dupefilter/filtered', spider=spider)
