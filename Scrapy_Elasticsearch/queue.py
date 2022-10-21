from scrapy.utils.reqser import request_to_dict, request_from_dict
from ast import literal_eval
from . import defaultjson

class Base(object):
    """Per-spider base queue class"""

    def __init__(self, server, els_index, spider):


        self.server = server
        self.els_index = els_index
        self.spider = spider
        self.search_q = defaultjson.search_type % ('request')

    def _encode_request(self, request):
        """Encode a request object"""
        obj = request_to_dict(request, self.spider)

        return obj

    def _decode_request(self, encoded_request):
        """Decode an request previously encoded"""
        return request_from_dict(encoded_request, self.spider)

    def __len__(self):
        """Return the length of the queue"""
        raise NotImplementedError

    def push(self, request):
        """defaults a request"""
        raise NotImplementedError

    def pop(self):
        """Pop a request"""
        raise NotImplementedError

    def clear(self):
        """Clear queue/stack"""
        print('CLEAR')


class FifoQueue(Base):

    def __len__(self):
        count = self.server.search(index=self.els_index, body=self.search_q, filter_path=['hits.total.value'])
        return count['hits']['total']['value']

    def push(self, request):
        """Push a request"""
        insert = defaultjson.insert.copy()
        insert['rule'] = 'request'
        insert['url'] = str(self._encode_request(request))

        self.server.index(index=self.els_index, body=insert)

    def pop(self):
        """Pop a request"""
        self.server.indices.refresh(index=self.els_index)
        data = self.server.search(index=self.els_index, body=self.search_q, filter_path=['hits.hits._source.url','hits.hits._id'], size=1)
        if data:
            self.server.delete(index=self.els_index, id= data['hits']['hits'][0]['_id'], ignore=[404])
            data = ''.join(data['hits']['hits'][0]['_source'].get('url'))
            return self._decode_request(literal_eval(data))


# TODO: Deprecate request_tmp use of these names.
SpiderQueue = FifoQueue

