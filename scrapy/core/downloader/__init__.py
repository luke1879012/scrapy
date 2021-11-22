import random
from time import time
from datetime import datetime
from collections import deque

from twisted.internet import defer, task

from scrapy.utils.defer import mustbe_deferred
from scrapy.utils.httpobj import urlparse_cached
from scrapy.resolver import dnscache
from scrapy import signals
from scrapy.core.downloader.middleware import DownloaderMiddlewareManager
from scrapy.core.downloader.handlers import DownloadHandlers


class Slot:
    """Downloader slot"""

    def __init__(self, concurrency, delay, randomize_delay):
        # 最大并发量
        self.concurrency = concurrency
        self.delay = delay
        self.randomize_delay = randomize_delay

        # 正在处理的请求
        self.active = set()
        # 未处理的请求
        self.queue = deque()
        # 正在传输响应的请求
        self.transferring = set()
        # 最近看过slot的时间
        self.lastseen = 0

        self.latercall = None

    def free_transfer_slots(self):
        return self.concurrency - len(self.transferring)

    def download_delay(self):
        if self.randomize_delay:
            # 返回0.5-1.5倍的延迟
            return random.uniform(0.5 * self.delay, 1.5 * self.delay)
        return self.delay

    def close(self):
        if self.latercall and self.latercall.active():
            self.latercall.cancel()

    def __repr__(self):
        cls_name = self.__class__.__name__
        return (f"{cls_name}(concurrency={self.concurrency!r}, "
                f"delay={self.delay:.2f}, "
                f"randomize_delay={self.randomize_delay!r})")

    def __str__(self):
        return (
            f"<downloader.Slot concurrency={self.concurrency!r} "
            f"delay={self.delay:.2f} randomize_delay={self.randomize_delay!r} "
            f"len(active)={len(self.active)} len(queue)={len(self.queue)} "
            f"len(transferring)={len(self.transferring)} "
            f"lastseen={datetime.fromtimestamp(self.lastseen).isoformat()}>"
        )


def _get_concurrency_delay(concurrency, spider, settings):
    # 下载的延迟
    delay = settings.getfloat('DOWNLOAD_DELAY')
    if hasattr(spider, 'download_delay'):
        delay = spider.download_delay

    if hasattr(spider, 'max_concurrent_requests'):
        concurrency = spider.max_concurrent_requests

    return concurrency, delay


class Downloader:

    DOWNLOAD_SLOT = 'download_slot'

    def __init__(self, crawler):
        self.settings = crawler.settings
        self.signals = crawler.signals
        self.slots = {}
        self.active = set()
        self.handlers = DownloadHandlers(crawler)
        self.total_concurrency = self.settings.getint('CONCURRENT_REQUESTS')

        # 每个域名的最大并发数量
        self.domain_concurrency = self.settings.getint('CONCURRENT_REQUESTS_PER_DOMAIN')
        # 每个ip的最大并发数量
        self.ip_concurrency = self.settings.getint('CONCURRENT_REQUESTS_PER_IP')

        # 是否开始随机延迟
        self.randomize_delay = self.settings.getbool('RANDOMIZE_DOWNLOAD_DELAY')
        # 初始化下载中间件
        self.middleware = DownloaderMiddlewareManager.from_crawler(crawler)
        self._slot_gc_loop = task.LoopingCall(self._slot_gc)
        self._slot_gc_loop.start(60)

    def fetch(self, request, spider):
        def _deactivate(response):
            self.active.remove(request)
            return response

        self.active.add(request)
        dfd = self.middleware.download(self._enqueue_request, request, spider)
        return dfd.addBoth(_deactivate)

    def needs_backout(self):
        # 现在下载的请求 比 配置的最大请求量 多，返回True
        return len(self.active) >= self.total_concurrency

    def _get_slot(self, request, spider):
        # 为了维护 一个key 一个slot，key为ip或域名
        key = self._get_slot_key(request, spider)
        if key not in self.slots:
            conc = self.ip_concurrency if self.ip_concurrency else self.domain_concurrency
            # 获取延迟和最大请求量
            conc, delay = _get_concurrency_delay(conc, spider, self.settings)
            self.slots[key] = Slot(conc, delay, self.randomize_delay)

        return key, self.slots[key]

    def _get_slot_key(self, request, spider):
        if self.DOWNLOAD_SLOT in request.meta:
            return request.meta[self.DOWNLOAD_SLOT]

        key = urlparse_cached(request).hostname or ''
        if self.ip_concurrency:
            key = dnscache.get(key, key)

        return key

    def _enqueue_request(self, request, spider):
        # 这里是加到slot中 正在处理的请求
        key, slot = self._get_slot(request, spider)
        request.meta[self.DOWNLOAD_SLOT] = key

        def _deactivate(response):
            slot.active.remove(request)
            return response

        slot.active.add(request)
        # s.request_reached_downloader 请求已到达下载器
        self.signals.send_catch_log(signal=signals.request_reached_downloader,
                                    request=request,
                                    spider=spider)
        deferred = defer.Deferred().addBoth(_deactivate)
        # 将所有请求加入队列
        slot.queue.append((request, deferred))
        self._process_queue(spider, slot)
        return deferred

    def _process_queue(self, spider, slot):
        from twisted.internet import reactor
        if slot.latercall and slot.latercall.active():
            return

        # Delay queue processing if a download_delay is configured
        # 如果配置了download_delay，则延迟队列处理
        now = time()
        delay = slot.download_delay()
        if delay:
            # 计算延迟操作(第一次请求肯定不用延迟，所以不用考虑slot.lastseen)
            penalty = delay - now + slot.lastseen
            if penalty > 0:
                slot.latercall = reactor.callLater(penalty, self._process_queue, spider, slot)
                return

        # Process enqueued requests if there are free slots to transfer for this slot
        while slot.queue and slot.free_transfer_slots() > 0:
            # slot并发数量
            slot.lastseen = now
            # 抛出队列
            request, deferred = slot.queue.popleft()
            dfd = self._download(slot, request, spider)
            # 结束时，将request对象从正在
            dfd.chainDeferred(deferred)
            # prevent burst if inter-request delays were configured
            if delay:
                self._process_queue(spider, slot)
                break

    def _download(self, slot, request, spider):
        # The order is very important for the following deferreds. Do not change!

        # 1. Create the download deferred
        dfd = mustbe_deferred(self.handlers.download_request, request, spider)

        # 2. Notify response_downloaded listeners about the recent download
        # before querying queue for next request
        # 在查询队列中的下一个请求之前，通知response_downloaded的侦听器有关最近的下载的信息
        def _downloaded(response):
            # s.response_downloaded 正在获取响应的位置
            self.signals.send_catch_log(signal=signals.response_downloaded,
                                        response=response,
                                        request=request,
                                        spider=spider)
            return response
        dfd.addCallback(_downloaded)

        # 3. After response arrives, remove the request from transferring
        # state to free up the transferring slot so it can be used by the
        # following requests (perhaps those which came from the downloader
        # middleware itself)
        # 响应到达后，将请求从传输状态中删除以释放传输插槽，以便随后的请求可以使用它（也许来自下载器中间件本身的请求）
        slot.transferring.add(request)

        def finish_transferring(_):
            slot.transferring.remove(request)
            self._process_queue(spider, slot)
            # s.request_left_downloader 刚离开下载器
            self.signals.send_catch_log(signal=signals.request_left_downloader,
                                        request=request,
                                        spider=spider)
            return _

        return dfd.addBoth(finish_transferring)

    def close(self):
        self._slot_gc_loop.stop()
        for slot in self.slots.values():
            slot.close()

    # 60s的定时心跳函数, 垃圾回收
    def _slot_gc(self, age=60):
        mintime = time() - age
        for key, slot in list(self.slots.items()):
            if not slot.active and slot.lastseen + slot.delay < mintime:
                self.slots.pop(key).close()
