from scrapy.commands import BaseRunSpiderCommand
from scrapy.exceptions import UsageError


class Command(BaseRunSpiderCommand):

    requires_project = True

    def syntax(self):
        return "[options] <spider>"

    def short_desc(self):
        # 描述命令
        return "Run a spider"

    def run(self, args, opts):
        if len(args) < 1:
            raise UsageError()
        elif len(args) > 1:
            raise UsageError("running 'scrapy crawl' with more than one spider is not supported")
        # 爬虫里的name属性
        spname = args[0]

        # 运行爬虫，生成defer
        crawl_defer = self.crawler_process.crawl(spname, **opts.spargs)

        # 没报错，则开始事件循环
        if getattr(crawl_defer, 'result', None) is not None and issubclass(crawl_defer.result.type, Exception):
            # 报错则退出, 标记退出代码不正常，为1
            self.exitcode = 1
        else:
            # 开始事件循环
            self.crawler_process.start()

            if (
                self.crawler_process.bootstrap_failed
                or hasattr(self.crawler_process, 'has_exception') and self.crawler_process.has_exception
            ):
                self.exitcode = 1
