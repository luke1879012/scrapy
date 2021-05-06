import os
import json
import logging
from os.path import join, exists

from scrapy.utils.misc import load_object, create_instance
from scrapy.utils.job import job_dir


logger = logging.getLogger(__name__)


class Scheduler:
    """
    Scrapy Scheduler. It allows to enqueue requests and then get
    a next request to download. Scheduler is also handling duplication
    filtering, via dupefilter.
    Scrapy Scheduler。它允许排队请求，然后获得下一个下载请求。调度程序还通过dupefilter处理重复过滤。

    Prioritization and queueing is not performed by the Scheduler.
    User sets ``priority`` field for each Request, and a PriorityQueue
    (defined by :setting:`SCHEDULER_PRIORITY_QUEUE`) uses these priorities
    to dequeue requests in a desired order.
    调度程序不执行优先级排序和排队。用户为每个请求设置``priority``字段，
    并且PriorityQueue（由SCHEDULER_PRIORITY_QUEUE定义）使用这些优先级以所需顺序将请求出队。

    Scheduler uses two PriorityQueue instances, configured to work in-memory
    and on-disk (optional). When on-disk queue is present, it is used by
    default, and an in-memory queue is used as a fallback for cases where
    a disk queue can't handle a request (can't serialize it).
    Scheduler使用两个PriorityQueue实例，这些实例配置为在内存和磁盘上运行（可选）。
    如果存在磁盘队列，则默认情况下使用它，并且在磁盘队列无法处理请求（无法序列化）的情况下，将内存队列用作备用。

    :setting:`SCHEDULER_MEMORY_QUEUE` and
    :setting:`SCHEDULER_DISK_QUEUE` allow to specify lower-level queue classes
    which PriorityQueue instances would be instantiated with, to keep requests
    on disk and in memory respectively.

    Overall, Scheduler is an object which holds several PriorityQueue instances
    (in-memory and on-disk) and implements fallback logic for them.
    Also, it handles dupefilters.
    总体而言，调度程序是一个对象，其中包含多个PriorityQueue实例（内存中和磁盘上），
    并为它们实现后备逻辑。此外，它还可以处理dupefilters。
    """

    @classmethod
    def from_crawler(cls, crawler):
        settings = crawler.settings
        # 加载去重类，并且创建实例
        dupefilter_cls = load_object(settings['DUPEFILTER_CLASS'])
        dupefilter = create_instance(dupefilter_cls, settings, crawler)
        # 优先级队列
        pqclass = load_object(settings['SCHEDULER_PRIORITY_QUEUE'])
        # 磁盘队列
        dqclass = load_object(settings['SCHEDULER_DISK_QUEUE'])
        # 内存队列
        mqclass = load_object(settings['SCHEDULER_MEMORY_QUEUE'])
        # 调度器日志
        logunser = settings.getbool('SCHEDULER_DEBUG')
        return cls(dupefilter, jobdir=job_dir(settings), logunser=logunser,
                   stats=crawler.stats, pqclass=pqclass, dqclass=dqclass,
                   mqclass=mqclass, crawler=crawler)

    def __init__(self, dupefilter, jobdir=None, dqclass=None, mqclass=None,
                 logunser=False, stats=None, pqclass=None, crawler=None):
        self.df = dupefilter
        # 生成磁盘的路径
        self.dqdir = self._dqdir(jobdir)
        self.pqclass = pqclass
        self.dqclass = dqclass
        self.mqclass = mqclass
        self.logunser = logunser
        self.stats = stats
        self.crawler = crawler

    def open(self, spider):
        self.spider = spider
        self.mqs = self._mq()
        # 如果存在磁盘路径，则实例化，否则为None
        self.dqs = self._dq() if self.dqdir else None
        return self.df.open()

    def __len__(self):
        return len(self.dqs) + len(self.mqs) if self.dqs else len(self.mqs)

    def has_pending_requests(self):
        # 判断是否 有待处理 的请求
        return len(self) > 0

    def close(self, reason):
        if self.dqs:
            state = self.dqs.close()
            # 如果磁盘队列还有东西，说明需要保存，断点恢复
            self._write_dqs_state(self.dqdir, state)
        return self.df.close(reason)

    def enqueue_request(self, request):
        if not request.dont_filter and self.df.request_seen(request):
            self.df.log(request, self.spider)
            return False
        dqok = self._dqpush(request)
        if dqok:
            self.stats.inc_value('scheduler/enqueued/disk', spider=self.spider)
        else:
            self._mqpush(request)
            self.stats.inc_value('scheduler/enqueued/memory', spider=self.spider)
        self.stats.inc_value('scheduler/enqueued', spider=self.spider)
        return True

    def next_request(self):
        request = self.mqs.pop()
        if request:
            self.stats.inc_value('scheduler/dequeued/memory', spider=self.spider)
        else:
            request = self._dqpop()
            if request:
                self.stats.inc_value('scheduler/dequeued/disk', spider=self.spider)
        if request:
            self.stats.inc_value('scheduler/dequeued', spider=self.spider)
        return request

    def _dqpush(self, request):
        if self.dqs is None:
            return
        try:
            self.dqs.push(request)
        except ValueError as e:  # non serializable request
            if self.logunser:
                msg = ("Unable to serialize request: %(request)s - reason:"
                       " %(reason)s - no more unserializable requests will be"
                       " logged (stats being collected)")
                logger.warning(msg, {'request': request, 'reason': e},
                               exc_info=True, extra={'spider': self.spider})
                self.logunser = False
            self.stats.inc_value('scheduler/unserializable',
                                 spider=self.spider)
            return
        else:
            return True

    def _mqpush(self, request):
        self.mqs.push(request)

    def _dqpop(self):
        if self.dqs:
            return self.dqs.pop()

    def _mq(self):
        """ Create a new priority queue instance, with in-memory storage """
        # 创建具有内存的新优先级队列实例
        return create_instance(self.pqclass,
                               settings=None,
                               crawler=self.crawler,
                               downstream_queue_cls=self.mqclass,
                               key='')

    def _dq(self):
        """ Create a new priority queue instance, with disk storage """
        # 使用磁盘存储创建一个新的优先级队列实例

        # 读取已存在的状态，用于断点恢复
        state = self._read_dqs_state(self.dqdir)
        q = create_instance(self.pqclass,
                            settings=None,
                            crawler=self.crawler,
                            downstream_queue_cls=self.dqclass,
                            key=self.dqdir,
                            startprios=state)
        if q:
            logger.info("Resuming crawl (%(queuesize)d requests scheduled)",
                        {'queuesize': len(q)}, extra={'spider': self.spider})
        return q

    def _dqdir(self, jobdir):
        """ Return a folder name to keep disk queue state at """
        # 返回文件夹名称，将磁盘队列状态保持在这个文件夹中
        if jobdir:
            dqdir = join(jobdir, 'requests.queue')
            if not exists(dqdir):
                os.makedirs(dqdir)
            return dqdir

    def _read_dqs_state(self, dqdir):
        path = join(dqdir, 'active.json')
        if not exists(path):
            return ()
        with open(path) as f:
            return json.load(f)

    def _write_dqs_state(self, dqdir, state):
        with open(join(dqdir, 'active.json'), 'w') as f:
            json.dump(state, f)
