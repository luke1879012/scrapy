import logging
import pprint
import signal
import warnings

from twisted.internet import defer
from zope.interface.exceptions import DoesNotImplement

try:
    # zope >= 5.0 only supports MultipleInvalid
    from zope.interface.exceptions import MultipleInvalid
except ImportError:
    MultipleInvalid = None

from zope.interface.verify import verifyClass

from scrapy import signals, Spider
from scrapy.core.engine import ExecutionEngine
from scrapy.exceptions import ScrapyDeprecationWarning
from scrapy.extension import ExtensionManager
from scrapy.interfaces import ISpiderLoader
from scrapy.settings import overridden_settings, Settings
from scrapy.signalmanager import SignalManager
from scrapy.utils.log import (
    configure_logging,
    get_scrapy_root_handler,
    install_scrapy_root_handler,
    log_scrapy_info,
    LogCounterHandler,
)
from scrapy.utils.misc import create_instance, load_object
from scrapy.utils.ossignal import install_shutdown_handlers, signal_names
from scrapy.utils.reactor import install_reactor, verify_installed_reactor


logger = logging.getLogger(__name__)


class Crawler:
    # 这个类相当于全局变量
    def __init__(self, spidercls, settings=None):
        if isinstance(spidercls, Spider):
            # spidercls参数必须是类，而不是对象
            raise ValueError('The spidercls argument must be a class, not an object')

        if isinstance(settings, dict) or settings is None:
            settings = Settings(settings)

        self.spidercls = spidercls
        # 加载配置文件
        self.settings = settings.copy()
        # 这里调用了更新设置
        self.spidercls.update_settings(self.settings)

        # 初始化信号管理类
        self.signals = SignalManager(self)
        # 初始化日志收集类
        self.stats = load_object(self.settings['STATS_CLASS'])(self)

        handler = LogCounterHandler(self, level=self.settings.get('LOG_LEVEL'))
        logging.root.addHandler(handler)

        d = dict(overridden_settings(self.settings))
        logger.info("Overridden settings:\n%(settings)s",
                    {'settings': pprint.pformat(d)})

        # 这个是关于根日志的
        if get_scrapy_root_handler() is not None:
            # scrapy root handler already installed: update it with new settings
            install_scrapy_root_handler(self.settings)
        # lambda is assigned to Crawler attribute because this way it is not
        # garbage collected after leaving __init__ scope
        # 加lambda为了防止垃圾回收机制？
        self.__remove_handler = lambda: logging.root.removeHandler(handler)
        self.signals.connect(self.__remove_handler, signals.engine_stopped)

        # 加载日志格式化的东西
        lf_cls = load_object(self.settings['LOG_FORMATTER'])
        self.logformatter = lf_cls.from_crawler(self)
        # 好像是个扩展
        self.extensions = ExtensionManager.from_crawler(self)

        # settings初始化完成，禁止修改了
        self.settings.freeze()
        # 未开始抓取
        self.crawling = False
        # 这里做准备操作，后面crawl才进行赋值
        self.spider = None
        self.engine = None

    @defer.inlineCallbacks
    def crawl(self, *args, **kwargs):
        # p.7 调用此方法，生成一个d
        if self.crawling:
            raise RuntimeError("Crawling already taking place")
        self.crawling = True

        try:
            # p.8 实例化spider
            self.spider = self._create_spider(*args, **kwargs)
            # p.10 创建引擎
            self.engine = self._create_engine()

            # p.12 生成最开始的请求，并转成迭代器
            start_requests = iter(self.spider.start_requests())
            # p.13 生成抓爬请求的行为
            yield self.engine.open_spider(self.spider, start_requests)
            # p.23 一切准备工作完成，启动引擎
            yield defer.maybeDeferred(self.engine.start)
        except Exception:
            self.crawling = False
            if self.engine is not None:
                yield self.engine.close()
            raise

    def _create_spider(self, *args, **kwargs):
        # p.9 调用spider类的from_crawler方法，实例化
        return self.spidercls.from_crawler(self, *args, **kwargs)

    def _create_engine(self):
        # p.11 实例化引擎,并挂载引擎正常停止的回调
        return ExecutionEngine(self, lambda _: self.stop())

    @defer.inlineCallbacks
    def stop(self):
        """Starts a graceful stop of the crawler and returns a deferred that is
        fired when the crawler is stopped.
        启动搜寻器的正常停止，并返回在搜寻器停止时触发的延迟。
        """
        if self.crawling:
            self.crawling = False
            yield defer.maybeDeferred(self.engine.stop)


class CrawlerRunner:
    """
    This is a convenient helper class that keeps track of, manages and runs
    crawlers inside an already setup :mod:`~twisted.internet.reactor`.
    这是一个方便的帮助程序类，可以在已经设置的〜twisted.internet.reactor中跟踪，管理和运行搜寻器。

    The CrawlerRunner object must be instantiated with a
    :class:`~scrapy.settings.Settings` object.
    必须使用：class：`〜scrapy.settings.Settings`对象实例化CrawlerRunner对象。

    This class shouldn't be needed (since Scrapy is responsible of using it
    accordingly) unless writing scripts that manually handle the crawling
    process. See :ref:`run-from-script` for an example.
    除非编写手动处理爬网过程的脚本，否则不需要该类（因为Scrapy负责相应地使用它）。有关示例，请参见-run-from-script`。
    """

    crawlers = property(
        lambda self: self._crawlers,
        doc="Set of :class:`crawlers <scrapy.crawler.Crawler>` started by "
            ":meth:`crawl` and managed by this class."
    )

    @staticmethod
    def _get_spider_loader(settings):
        """ Get SpiderLoader instance from settings """
        # 从设置中获取SpiderLoader实例
        cls_path = settings.get('SPIDER_LOADER_CLASS')
        loader_cls = load_object(cls_path)
        excs = (DoesNotImplement, MultipleInvalid) if MultipleInvalid else DoesNotImplement
        try:
            verifyClass(ISpiderLoader, loader_cls)
        except excs:
            warnings.warn(
                'SPIDER_LOADER_CLASS (previously named SPIDER_MANAGER_CLASS) does '
                'not fully implement scrapy.interfaces.ISpiderLoader interface. '
                'Please add all missing methods to avoid unexpected runtime errors.',
                category=ScrapyDeprecationWarning, stacklevel=2
            )
        return loader_cls.from_settings(settings.frozencopy())

    def __init__(self, settings=None):
        if isinstance(settings, dict) or settings is None:
            settings = Settings(settings)
        self.settings = settings
        self.spider_loader = self._get_spider_loader(settings)
        self._crawlers = set()
        self._active = set()
        self.bootstrap_failed = False
        self._handle_twisted_reactor()

    @property
    def spiders(self):
        warnings.warn("CrawlerRunner.spiders attribute is renamed to "
                      "CrawlerRunner.spider_loader.",
                      category=ScrapyDeprecationWarning, stacklevel=2)
        return self.spider_loader

    def crawl(self, crawler_or_spidercls, *args, **kwargs):  # 先运行CrawlerRunner.crawl方法，再运行CrawlerProcess.start方法
        """
        Run a crawler with the provided arguments.
        使用提供的参数运行搜寻器。

        It will call the given Crawler's :meth:`~Crawler.crawl` method, while
        keeping track of it so it can be stopped later.
        它会调用给定的Crawler的：meth：`〜Crawler.crawl方法，同时对其进行跟踪，以便稍后将其停止。

        If ``crawler_or_spidercls`` isn't a :class:`~scrapy.crawler.Crawler`
        instance, this method will try to create one using this parameter as
        the spider class given to it.
        如果crawler_or_spidercls不是一个：class：〜scrapy.crawler.Crawler实例，
        则此方法将尝试使用此参数作为为其指定的Spider类来创建一个。

        Returns a deferred that is fired when the crawling is finished.
        返回在爬网完成时触发的延迟。

        :param crawler_or_spidercls: already created crawler, or a spider class
            or spider's name inside the project to create it
            已经创建的搜寻器，或在项目内部创建的蜘蛛类或蜘蛛的名称
        :type crawler_or_spidercls: :class:`~scrapy.crawler.Crawler` instance,
            :class:`~scrapy.spiders.Spider` subclass or string

        :param args: arguments to initialize the spider

        :param kwargs: keyword arguments to initialize the spider
        """
        if isinstance(crawler_or_spidercls, Spider):
            raise ValueError(
                'The crawler_or_spidercls argument cannot be a spider object, '
                'it must be a spider class (or a Crawler object)')
        # p.1 创建crawler
        crawler = self.create_crawler(crawler_or_spidercls)
        # p.4 得到crawler实例后，运行
        return self._crawl(crawler, *args, **kwargs)

    def _crawl(self, crawler, *args, **kwargs):
        # p.5 加入crawlers这个集合中. crawlers表示正在运行的crawler
        self.crawlers.add(crawler)
        # p.6 调用crawler实例的crawl方法，并生成一个d（我理解为生成器，是所有抓爬的行为集合）
        d = crawler.crawl(*args, **kwargs)
        # 将d加入到
        self._active.add(d)

        def _done(result):
            self.crawlers.discard(crawler)
            self._active.discard(d)
            self.bootstrap_failed |= not getattr(crawler, 'spider', None)
            return result

        return d.addBoth(_done)

    def create_crawler(self, crawler_or_spidercls):
        """
        Return a :class:`~scrapy.crawler.Crawler` object.

        * If ``crawler_or_spidercls`` is a Crawler, it is returned as-is.
        * If ``crawler_or_spidercls`` is a Spider subclass, a new Crawler
          is constructed for it.
        * If ``crawler_or_spidercls`` is a string, this function finds
          a spider with this name in a Scrapy project (using spider loader),
          then creates a Crawler instance for it.
        """
        if isinstance(crawler_or_spidercls, Spider):
            raise ValueError(
                'The crawler_or_spidercls argument cannot be a spider object, '
                'it must be a spider class (or a Crawler object)')
        if isinstance(crawler_or_spidercls, Crawler):
            return crawler_or_spidercls
        # p.2 加判断，如果是实例则返回，不是实例则创建
        return self._create_crawler(crawler_or_spidercls)

    def _create_crawler(self, spidercls):
        if isinstance(spidercls, str):
            spidercls = self.spider_loader.load(spidercls)
        # p.3 根据传入转成crawler类
        return Crawler(spidercls, self.settings)

    def stop(self):
        """
        Stops simultaneously all the crawling jobs taking place.

        Returns a deferred that is fired when they all have ended.
        """
        return defer.DeferredList([c.stop() for c in list(self.crawlers)])

    @defer.inlineCallbacks
    def join(self):
        """
        join()

        Returns a deferred that is fired when all managed :attr:`crawlers` have
        completed their executions.
        返回所有托管的爬网程序完成执行后触发的延迟。
        """
        while self._active:
            yield defer.DeferredList(self._active)

    def _handle_twisted_reactor(self):
        if self.settings.get("TWISTED_REACTOR"):
            verify_installed_reactor(self.settings["TWISTED_REACTOR"])


class CrawlerProcess(CrawlerRunner):
    """
    A class to run multiple scrapy crawlers in a process simultaneously.
    在一个进程中同时运行多个scrapy爬网程序的类。

    This class extends :class:`~scrapy.crawler.CrawlerRunner` by adding support
    for starting a :mod:`~twisted.internet.reactor` and handling shutdown
    signals, like the keyboard interrupt command Ctrl-C. It also configures
    top-level logging.
    此类扩展了：class：`〜scrapy.crawler.CrawlerRunner`，它增加了对启动：mod：`〜twisted.internet.reactor的支持并处理关闭信号，
    例如键盘中断命令Ctrl-C。它还配置顶级日志记录。

    This utility should be a better fit than
    :class:`~scrapy.crawler.CrawlerRunner` if you aren't running another
    :mod:`~twisted.internet.reactor` within your application.
    如果您没有在应用程序中运行另一个：mod：`〜twisted.internet.reactor`，那么该实用程序应该比：scrapy.crawler.CrawlerRunner更合适。

    The CrawlerProcess object must be instantiated with a
    :class:`~scrapy.settings.Settings` object.
    必须使用：class：`〜scrapy.settings.Settings`对象实例化CrawlerProcess对象。

    :param install_root_handler: whether to install root logging handler 安装根日志记录处理程序
        (default: True)

    This class shouldn't be needed (since Scrapy is responsible of using it
    accordingly) unless writing scripts that manually handle the crawling
    process. See :ref:`run-from-script` for an example.
    除非编写手动处理爬网过程的脚本，否则不需要该类（因为Scrapy负责相应地使用它）。有关示例，请参见-run-from-script`。
    """

    def __init__(self, settings=None, install_root_handler=True):
        super().__init__(settings)
        install_shutdown_handlers(self._signal_shutdown)
        configure_logging(self.settings, install_root_handler)
        log_scrapy_info(self.settings)

    def _signal_shutdown(self, signum, _):
        from twisted.internet import reactor
        install_shutdown_handlers(self._signal_kill)
        signame = signal_names[signum]
        logger.info("Received %(signame)s, shutting down gracefully. Send again to force ",
                    {'signame': signame})
        reactor.callFromThread(self._graceful_stop_reactor)

    def _signal_kill(self, signum, _):
        from twisted.internet import reactor
        install_shutdown_handlers(signal.SIG_IGN)
        signame = signal_names[signum]
        logger.info('Received %(signame)s twice, forcing unclean shutdown',
                    {'signame': signame})
        reactor.callFromThread(self._stop_reactor)

    def start(self, stop_after_crawl=True):
        """
        This method starts a :mod:`~twisted.internet.reactor`, adjusts its pool
        size to :setting:`REACTOR_THREADPOOL_MAXSIZE`, and installs a DNS cache
        based on :setting:`DNSCACHE_ENABLED` and :setting:`DNSCACHE_SIZE`.
        此方法启动〜twisted.internet.reactor，将其池大小调整为REACTOR_THREADPOOL_MAXSIZE，
        并基于DNSCACHE_ENABLED和DNSCACHE_SIZE设置DNS缓存。

        If ``stop_after_crawl`` is True, the reactor will be stopped after all
        crawlers have finished, using :meth:`join`.
        如果stop_after_crawl为True，则所有爬网程序完成后，将使用：meth：`join停止反应堆。

        :param bool stop_after_crawl: stop or not the reactor when all
            crawlers have finished
            所有履带完成后，反应堆是否停止
        """
        from twisted.internet import reactor
        if stop_after_crawl:
            # p.26 将 爬虫行为 加入deferList中
            d = self.join()
            # Don't start the reactor if the deferreds are already fired
            if d.called:
                return
            # p.27 添加停止事件循环的回调
            d.addBoth(self._stop_reactor)

        resolver_class = load_object(self.settings["DNS_RESOLVER"])
        resolver = create_instance(resolver_class, self.settings, self, reactor=reactor)
        resolver.install_on_reactor()
        tp = reactor.getThreadPool()
        tp.adjustPoolsize(maxthreads=self.settings.getint('REACTOR_THREADPOOL_MAXSIZE'))
        reactor.addSystemEventTrigger('before', 'shutdown', self.stop)
        # p.28 开启事件循环
        reactor.run(installSignalHandlers=False)  # blocking call

    def _graceful_stop_reactor(self):
        d = self.stop()
        d.addBoth(self._stop_reactor)
        return d

    def _stop_reactor(self, _=None):
        from twisted.internet import reactor
        try:
            reactor.stop()
        except RuntimeError:  # raised if already stopped or in shutdown stage
            pass

    def _handle_twisted_reactor(self):
        if self.settings.get("TWISTED_REACTOR"):
            install_reactor(self.settings["TWISTED_REACTOR"], self.settings["ASYNCIO_EVENT_LOOP"])
        super()._handle_twisted_reactor()
