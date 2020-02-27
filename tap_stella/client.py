import datetime
import enum
import json
import logging
import os
import time
import urllib.parse

from dateutil.parser import parse as parse_datetime
import jwt
import requests
import requests.exceptions
import singer

LOGGER = singer.get_logger()


def set_query_parameters(url, **params):
    """Given a URL, set or replace a query parameter and return the
    modified URL.

    >>> set_query_parameters('http://example.com?foo=bar&biz=baz', foo='stuff', bat='boots')
    'http://example.com?foo=stuff&biz=baz&bat=boots'

    """
    scheme, netloc, path, query_string, fragment = urllib.parse.urlsplit(url)
    query_params = urllib.parse.parse_qs(query_string)

    new_query_string = ''

    for param_name, param_value in params.items():
        query_params[param_name] = [param_value]
        new_query_string = urllib.parse.urlencode(query_params, doseq=True)

    return urllib.parse.urlunsplit((scheme, netloc, path, new_query_string, fragment))


class Client:
    BASE_URL = 'https://api.stellaconnect.net'
    MAX_GET_ATTEMPTS = 7

    def __init__(self, config):
        self.api_key = config['api_key']
        secret = config['secret']
        self.jwt_token = jwt.encode({}, secret, algorithm='HS256')

    def get_headers(self, extra_headers):
        headers = {
            'Authorization': self.jwt_token,
            'x-api-key': self.api_key
        }
        if extra_headers:
            headers.update(extra_headers)

        return headers

    def get(self, url, params=None, headers=None):
        if not url.startswith('https://'):
            url = f'{self.BASE_URL}/{url}'

        LOGGER.info(f'Stella connect GET {url}')

        for num_retries in range(self.MAX_GET_ATTEMPTS):
            will_retry = num_retries < self.MAX_GET_ATTEMPTS - 1
            try:
                resp = requests.get(url, params=params, headers=self.get_headers(headers))
            # Catch the base exception from requests
            except requests.exceptions.RequestException as e:
                resp = None
                if will_retry:
                    LOGGER.info('Stella connect: unable to get response, will retry', exc_info=True)
                else:
                    raise APIQueryError({'message': str(e)}) from e
            if will_retry:
                if resp and resp.status_code >= 500:
                    LOGGER.info('stella connect request with 5xx response, retrying', extra={
                        'url': resp.url,
                        'reason': resp.reason,
                        'code': resp.status_code
                    })
                # resp will be None if there was a `ConnectionError`
                elif resp is not None:
                    break  # No retry needed
                time.sleep(60)

        resp.raise_for_status()
        return resp.json()

    def paging_get(self, url, **get_args):
        requested_urls = set()

        sequence_id = 0
        MAX_RESPONSE_SIZE = 1000

        get_args = {k: v for k, v in get_args.items() if v is not None}
        url = set_query_parameters(url, **get_args)

        while url and url not in requested_urls:
            requested_urls.add(url)
            data = self.get(url)

            LOGGER.info('stella connect paging request', extra={
                'total_size': len(data),
                'page': len(requested_urls),
                'url': url,
            })

            for record in data:
                sequence_id = max(record['sequence_id'], sequence_id)

            yield sequence_id, data

            if len(data) < MAX_RESPONSE_SIZE:
                break

            url = set_query_parameters(url, after=sequence_id)
