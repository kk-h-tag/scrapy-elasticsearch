from .connection import get_els_from_settings
import logging

from . import defaultjson
from scrapy.dupefilters import BaseDupeFilter
from urllib.parse import quote


logger = logging.getLogger(__name__)


# TODO: Rename class to Elasticsearch DupeFilter.
class RFPDupeFilter(BaseDupeFilter):
    logger = logger

    def __init__(self, server, els_index , debug=False):
        self.server = server
        self.debug = debug
        self.els_index = els_index
        self.logdupes = True
    @classmethod
    def from_settings(cls, settings):
        server = get_els_from_settings(settings)
        index = settings.get('ELS_INDEX', '')
        debug = settings.getbool('DUPEFILTER_DEBUG')
        return cls(server, els_index=index,debug=debug)

    @classmethod
    def from_crawler(cls, crawler):
        return cls.from_settings(crawler.settings)

    def request_seen(self, request):
        added = 1
        # This returns the number of values add ed, zero if already exists.
        search_q = defaultjson.search_data % ('dup', quote(request.url))
        self.server.indices.refresh(index=self.els_index)
        result = self.server.search(index=self.els_index, body=search_q, filter_path=['hits.hits._source.url'])
        if result:
            added = 0
        else:
            insert = defaultjson.insert.copy()
            insert['rule'] = 'dup'
            insert['url'] = quote(request.url)
            self.server.index(index=self.els_index, body=insert)
        return added == 0

    def log(self, request, spider):
        if self.debug:
            msg = "Filtered duplicate request: %(request)s"
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
        elif self.logdupes:
            msg = ("Filtered duplicate request %(request)s"
                   " - no more duplicates will be shown"
                   " (see DUPEFILTER_DEBUG to show all duplicates)")
            self.logger.debug(msg, {'request': request}, extra={'spider': spider})
            self.logdupes = False
