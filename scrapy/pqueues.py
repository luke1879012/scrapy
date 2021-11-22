import hashlib
import logging

from scrapy.utils.misc import create_instance


logger = logging.getLogger(__name__)


def _path_safe(text):
    """
    Return a filesystem-safe version of a string ``text``
    返回文件系统安全版本的字符串``text``

    >>> _path_safe('simple.org').startswith('simple.org')
    True
    >>> _path_safe('dash-underscore_.org').startswith('dash-underscore_.org')
    True
    >>> _path_safe('some@symbol?').startswith('some_symbol_')
    True
    """
    pathable_slot = "".join([c if c.isalnum() or c in '-._' else '_' for c in text])
    # as we replace some letters we can get collision for different slots
    # add we add unique part
    unique_slot = hashlib.md5(text.encode('utf8')).hexdigest()
    return '-'.join([pathable_slot, unique_slot])


class ScrapyPriorityQueue:
    """A priority queue implemented using multiple internal queues (typically,
    FIFO queues). It uses one internal queue for each priority value. The internal
    queue must implement the following methods:

        * push(obj)
        * pop()
        * close()
        * __len__()

    Optionally, the queue could provide a ``peek`` method, that should return the
    next object to be returned by ``pop``, but without removing it from the queue.

    ``__init__`` method of ScrapyPriorityQueue receives a downstream_queue_cls
    argument, which is a class used to instantiate a new (internal) queue when
    a new priority is allocated.
    ScrapyPriorityQueue的``__init__``方法接收一个``downstream_queue_cls``参数，
    该参数是用于在分配新优先级时实例化新（内部）队列的类。

    Only integer priorities should be used. Lower numbers are higher
    priorities.
    仅应使用整数优先级。较低的数字是较高的优先级。

    startprios is a sequence of priorities to start with. If the queue was
    previously closed leaving some priority buckets non-empty, those priorities
    should be passed in startprios.
    startprios是一系列优先级的起点。
    如果该队列先前已关闭，而使某些优先级存储桶为非空，则这些优先级应在startprios中传递。

    """

    @classmethod
    def from_crawler(cls, crawler, downstream_queue_cls, key, startprios=()):
        return cls(crawler, downstream_queue_cls, key, startprios)

    def __init__(self, crawler, downstream_queue_cls, key, startprios=()):
        self.crawler = crawler
        # 队列的类
        self.downstream_queue_cls = downstream_queue_cls
        self.key = key
        self.queues = {}  # 键：优先级数字，值：队列实例
        self.curprio = None  # 最小的优先级数字
        self.init_prios(startprios)

    def init_prios(self, startprios):
        if not startprios:
            return

        for priority in startprios:
            self.queues[priority] = self.qfactory(priority)

        self.curprio = min(startprios)

    def qfactory(self, key):
        # 实例化队列
        return create_instance(
            self.downstream_queue_cls,
            None,
            self.crawler,
            self.key + '/' + str(key),
        )

    def priority(self, request):
        # 将优先级转为负数
        return -request.priority

    def push(self, request):
        # 找到对应的优先级队列
        priority = self.priority(request)
        if priority not in self.queues:
            # 如果不在这个字典里面，则赋值
            self.queues[priority] = self.qfactory(priority)
        # 找到对应的队列
        q = self.queues[priority]
        # 将这个请求推入
        q.push(request)  # this may fail (eg. serialization error)
        # 找到最小的优先级队列
        if self.curprio is None or priority < self.curprio:
            self.curprio = priority

    def pop(self):
        # 如果没有最小的优先级，则返回
        if self.curprio is None:
            return
        # 找到最小的优先级队列
        q = self.queues[self.curprio]
        # 推出请求
        m = q.pop()
        if not q:
            # 如果请求为空了，则删除字典里的这个键
            del self.queues[self.curprio]
            # 将队列回收
            q.close()
            # 导出所有的键（优先级）
            prios = [p for p, q in self.queues.items() if q]
            # 找到最小的优先级，并赋值
            self.curprio = min(prios) if prios else None
        return m

    def peek(self):
        """Returns the next object to be returned by :meth:`pop`,
        but without removing it from the queue.

        Raises :exc:`NotImplementedError` if the underlying queue class does
        not implement a ``peek`` method, which is optional for queues.
        """
        if self.curprio is None:
            return None
        queue = self.queues[self.curprio]
        return queue.peek()

    def close(self):
        active = []
        for p, q in self.queues.items():
            active.append(p)
            q.close()
        return active

    def __len__(self):
        return sum(len(x) for x in self.queues.values()) if self.queues else 0


class DownloaderInterface:

    def __init__(self, crawler):
        self.downloader = crawler.engine.downloader

    def stats(self, possible_slots):
        return [(self._active_downloads(slot), slot) for slot in possible_slots]

    def get_slot_key(self, request):
        return self.downloader._get_slot_key(request, None)

    def _active_downloads(self, slot):
        """ Return a number of requests in a Downloader for a given slot """
        if slot not in self.downloader.slots:
            return 0
        return len(self.downloader.slots[slot].active)


class DownloaderAwarePriorityQueue:
    """ PriorityQueue which takes Downloader activity into account:
    domains (slots) with the least amount of active downloads are dequeued
    first.
    """

    @classmethod
    def from_crawler(cls, crawler, downstream_queue_cls, key, startprios=()):
        return cls(crawler, downstream_queue_cls, key, startprios)

    def __init__(self, crawler, downstream_queue_cls, key, slot_startprios=()):
        if crawler.settings.getint('CONCURRENT_REQUESTS_PER_IP') != 0:
            raise ValueError(f'"{self.__class__}" does not support CONCURRENT_REQUESTS_PER_IP')

        if slot_startprios and not isinstance(slot_startprios, dict):
            raise ValueError("DownloaderAwarePriorityQueue accepts "
                             "``slot_startprios`` as a dict; "
                             f"{slot_startprios.__class__!r} instance "
                             "is passed. Most likely, it means the state is"
                             "created by an incompatible priority queue. "
                             "Only a crawl started with the same priority "
                             "queue class can be resumed.")

        self._downloader_interface = DownloaderInterface(crawler)
        self.downstream_queue_cls = downstream_queue_cls
        self.key = key
        self.crawler = crawler

        self.pqueues = {}  # slot -> priority queue
        for slot, startprios in (slot_startprios or {}).items():
            self.pqueues[slot] = self.pqfactory(slot, startprios)

    def pqfactory(self, slot, startprios=()):
        return ScrapyPriorityQueue(
            self.crawler,
            self.downstream_queue_cls,
            self.key + '/' + _path_safe(slot),
            startprios,
        )

    def pop(self):
        stats = self._downloader_interface.stats(self.pqueues)

        if not stats:
            return

        slot = min(stats)[1]
        queue = self.pqueues[slot]
        request = queue.pop()
        if len(queue) == 0:
            del self.pqueues[slot]
        return request

    def push(self, request):
        slot = self._downloader_interface.get_slot_key(request)
        if slot not in self.pqueues:
            self.pqueues[slot] = self.pqfactory(slot)
        queue = self.pqueues[slot]
        queue.push(request)

    def peek(self):
        """Returns the next object to be returned by :meth:`pop`,
        but without removing it from the queue.

        Raises :exc:`NotImplementedError` if the underlying queue class does
        not implement a ``peek`` method, which is optional for queues.
        """
        stats = self._downloader_interface.stats(self.pqueues)
        if not stats:
            return None
        slot = min(stats)[1]
        queue = self.pqueues[slot]
        return queue.peek()

    def close(self):
        active = {slot: queue.close() for slot, queue in self.pqueues.items()}
        self.pqueues.clear()
        return active

    def __len__(self):
        return sum(len(x) for x in self.pqueues.values()) if self.pqueues else 0

    def __contains__(self, slot):
        return slot in self.pqueues
