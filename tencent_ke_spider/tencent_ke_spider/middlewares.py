import requests


class IP_ProxyMiddleware(object):
    def process_request(self, request, spider):
        request.meta['proxy'] = "http://101.35.21.127:16128"
