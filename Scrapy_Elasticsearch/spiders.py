from scrapy import signals
from scrapy.exceptions import DontCloseSpider
from scrapy.spiders import CrawlSpider

from . import defaultjson
from . import connection
from .utils import bytes_to_str
from scrapy.http import Request

class ElasticsearchMixin(object):
    """Mixin class to implement reading urls from a Elasticsearch queue."""

    # Elasticsearch client placeholder.
    server = None
    elastic_batch_size = None
    crawler = None
    els_index = None

    def start_requests(self):
        """Returns a batch of start requests from Elasticsearch."""
        return self.next_requests()

    def setup_Elasticsearch(self, crawler=None):
        """Setup Elasticsearch connection and idle signal.

        This should be called after the spider has set its crawler object.
        """
        if self.server is not None:
            return

        if crawler is None:
            # We allow optional crawler argument to keep backwards
            # compatibility.
            # XXX: Raise a deprecation warning.
            crawler = getattr(self, 'crawler', None)

        if crawler is None:
            raise ValueError("crawler is required")

        self.settings = crawler.settings

        if self.els_index is None:
            self.els_index = self.settings.get(
                'ELS_INDEX', ''
            )


        if not self.els_index.strip():
            raise ValueError("els_index must not be empty")

        if self.elastic_batch_size is None:
            # TODO: Deprecate this setting (Elasticsearch_START_URLS_BATCH_SIZE).
            self.elastic_batch_size = self.settings.getint('CONCURRENT_REQUESTS')
        try:
            self.elastic_batch_size = int(self.elastic_batch_size)
        except (TypeError, ValueError):
            raise ValueError("Elasticsearch_batch_size must be an integer")

        self.server = connection.from_settings(crawler.settings)
        # The idle signal is called when the spider has no requests left,
        # that's when we will schedule new requests from Elasticsearch queue
        crawler.signals.connect(self.spider_idle, signal=signals.spider_idle)

    def next_requests(self):
        """Returns a request to be scheduled or none."""
        search_q = defaultjson.search_type % ('startUrl')

        # XXX: Do we need to use a timeout here?
        found = 0
        while found < self.elastic_batch_size:
            data = None
            self.server.indices.refresh(index=self.els_index)
            result = self.server.search(index=self.els_index, body=search_q, filter_path=['hits.hits._source.url','hits.hits._id'], size=1, timeout='30s')

            if result :
                self.server.delete(index=self.els_index, id=result['hits']['hits'][0]['_id'], ignore=[404])
                data = ''.join(result['hits']['hits'][0]['_source'].get('url'))

            if not data:
                break

            req = self.make_request_from_data(data)
            if req:
                yield req
                found += 1
            else:
                self.logger.debug("Request not made from data: %r", data)

        if found:
            self.logger.debug("Read %s requests", found)

    def make_request_from_data(self, data):
        url = bytes_to_str(data)
        return Request(url, dont_filter=True)

    def schedule_next_requests(self):
        """Schedules a request if available"""
        # TODO: While there is capacity, schedule a batch of Elasticsearch requests.
        for req in self.next_requests():
            self.crawler.engine.crawl(req, spider=self)

    def spider_idle(self):
        """Schedules a request if available, otherwise waits."""
        # XXX: Handle a sentinel to close the spider.
        self.schedule_next_requests()
        raise DontCloseSpider

class ElasticsearchCrawlSpider(ElasticsearchMixin, CrawlSpider):

    @classmethod
    def from_crawler(self, crawler, *args, **kwargs):
        obj = super(ElasticsearchCrawlSpider, self).from_crawler(crawler, *args, **kwargs)
        obj.setup_Elasticsearch(crawler)
        return obj

