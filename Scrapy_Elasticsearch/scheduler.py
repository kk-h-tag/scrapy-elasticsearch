from scrapy.utils.misc import load_object
import logging
from . import connection


# TODO: add SCRAPY_JOB support.
class Scheduler(object):

    def __init__(self, server,
                 queue_cls,
                 dupefilter_cls,
                 els_index,
                 idle_before_close=0):

        if idle_before_close < 0:
            raise TypeError("idle_before_close cannot be negative")

        self.server = server
        self.idle_before_close = idle_before_close
        self.queue_cls = queue_cls
        self.els_index = els_index
        self.dupefilter_cls = dupefilter_cls
        self.stats = None

    def __len__(self):
        return len(self.queue)

    @classmethod
    def from_settings(cls, settings):
        kwargs = {
            'idle_before_close': settings.getint('SCHEDULER_IDLE_BEFORE_CLOSE'),
        }

        # If these values are missing, it means we want to use the defaults.
        optional = {
            # TODO: Use custom prefixes for this settings to note that are
            # specific to scrapy-Elasticsearch
            'queue_cls': 'SCHEDULER_QUEUE_CLASS',
            # We use the default setting name to keep compatibility.
            'dupefilter_cls': 'DUPEFILTER_CLASS',
            'els_index': 'ELS_INDEX'
        }
        for name, setting_name in optional.items():
            val = settings.get(setting_name)
            if val:
                kwargs[name] = val

        server = connection.from_settings(settings)
        # Ensure the connection is working.

        return cls(server=server, **kwargs)

    @classmethod
    def from_crawler(cls, crawler):
        instance = cls.from_settings(crawler.settings)
        # FIXME: for now, stats are only supported from this constructor
        instance.stats = crawler.stats
        return instance

    def open(self, spider):
        self.spider = spider

        try:
            self.queue = load_object(self.queue_cls)(
                server=self.server,
                spider=spider,
                els_index=self.els_index
            )
        except TypeError as e:
            raise ValueError("Failed to instantiate queue class '%s': %s",
                             self.queue_cls, e)

        try:
            self.df = load_object(self.dupefilter_cls)(
                server=self.server,
                els_index=self.els_index,
                debug=spider.settings.getbool('DUPEFILTER_DEBUG'),
            )
        except TypeError as e:
            raise ValueError("Failed to instantiate dupefilter class '%s': %s",
                             self.dupefilter_cls, e)

        # notice if there are requests already in the queue to resume the crawl
        if len(self.queue):
            spider.log("Resuming crawl (%d requests scheduled)" % len(self.queue))

    def close(self, reason):
        logging.critical('Scheduler Close' + str(reason))

    def enqueue_request(self, request):
        if not request.dont_filter and self.df.request_seen(request):
            self.df.log(request, self.spider)
            return False
        if self.stats:
            self.stats.inc_value('scheduler/enqueued/elasticsearch', spider=self.spider)
        self.queue.push(request)
        return True

    def next_request(self):
        request = self.queue.pop()
        if request and self.stats:
            self.stats.inc_value('scheduler/dequeued/elasticsearch', spider=self.spider)
        return request

    def has_pending_requests(self):
        return len(self) > 0
