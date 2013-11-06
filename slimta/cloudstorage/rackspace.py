# Copyright (c) 2013 Ian C. Good
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
# THE SOFTWARE.
#

"""This module defines the queue storage mechanism specific to the `Rackspace
Cloud`_ hosting service. It requires an account as well as `Cloud Files`_ and
`Cloud Queues`_ services.

For each queued message, the contents of the message are written to *Cloud
Files*. Upon success, the message's metadata and a reference to the contents
are injected into *Cloud Queues* as a new message.

Workers will attempt to claim messages for processing. The expiration of the
claim will initially be set to the length of time allowed of the relaying
attempt. If relaying is successful or fails permanently, the queue message is
deleted. If relaying transiently fails but may be successful later, the claim
expiration is extended up to the next retry time.

.. _Rackspace Cloud: http://www.rackspace.com/cloud/
.. _Cloud Files: http://www.rackspace.com/cloud/files/
.. _Cloud Queues: http://www.rackspace.com/cloud/queues/

"""

from __future__ import absolute_import

import cPickle
import uuid
import json
from urlparse import urlsplit
from urllib import urlencode
from functools import partial

import gevent
from gevent.socket import getfqdn

from slimta.http import HTTPConnection, HTTPSConnection
from slimta.queue import QueueStorage
from slimta import logging
from . import CloudStorageError

__all__ = ['RackspaceStorage']

log = logging.getQueueStorageLogger(__name__)
http_log = logging.getHttpLogger(__name__)

_DEFAULT_AUTH_ENDPOINT = 'https://identity.api.rackspacecloud.com/v2.0/'
_DEFAULT_CLIENT_ID = uuid.uuid5(uuid.NAMESPACE_DNS, getfqdn())


class RackspaceError(CloudStorageError):
    """Base exception for all errors thrown by the Rackspace queue storage
    backend.

    """
    pass


class RackspaceResponseError(RackspaceError):
    """Thrown when an unexpected status has been returned from a Rackspace
    Cloud API request and the engine does not know how to continue.

    """

    def __init__(self, response):
        status = '{0!s} {1}'.format(response.status, response.reason)
        msg = 'Received {0!r} from the API.'.format(status)
        super(RackspaceResponseError, self).__init__(msg)

        #: The `~httplib.HTTPResponse` object that triggered the
        #: exception.
        self.response = response


class RackspaceStorage(QueueStorage):
    """Instances of this class may be used wherever a |QueueStorage| object is
    required. It will coordinate communication with the Rackspace Cloud Auth,
    Files, and Queues APIs.

    For the Rackspace Cloud Auth API, credentials are used to create a token.
    This token is then passed to all subsequent calls to other Rackspace Cloud
    APIs.  When the token is no longer valid, a new one is created.

    :param credentials: This dictionary defines how credentials are sent to the
                        Auth API.

                        If the ``function`` key is defined, it must be a
                        callable that takes no arguments and returns a tuple.
                        The tuple must contain a token string, a Cloud Queues
                        service endpoint, and a Cloud Queues service endpoint.

                        Otherwise, this dictionary must have a ``username`` key
                        whose value is the Rackspace Cloud username string.

                        The ``password`` key may be used to fetch tokens using
                        the account's password. Alternatively, the ``api_key``
                        key may be used to fetch tokens using the account's API
                        key. With ``username``, either ``password`` or
                        ``api_key`` must be given.

                        Optionally, ``tenant_id`` may also be provided for
                        situations where it is necessary for authentication.
    :type credentials: dict
    :param auth_endpoint: If given, this is the Rackspace Cloud Auth endpoint
                          to hit when creating tokens.
    :param auth_timeout: Timeout, in seconds, for requests to the Cloud Auth
                         API to create a new token for the session.
    :param queues_timeout: Timeout, in seconds, for all requests to the Cloud
                           Queues API.
    :param files_timeout: Timeout, in seconds, for all requests to the Cloud
                          Files API.
    :param queues_region: The Cloud Queues API region to use (e.g. ``IAD`` or
                          ``HKG``). By default, the first one discovered will
                          be used.
    :param files_region: The Cloud Files API region to use (e.g. ``IAD`` or
                         ``HKG``). By default, the first one discovered will be
                         used.
    :param queue_name: The Cloud Files queue name to use.
    :param container_name: The Cloud Files container name to use. The files in
                           this container will be named with random UUID
                           strings.
    :param client_id: The ``Client-ID`` header passed in with all Cloud Queues
                      requests. By default, this is generated using
                      :func:`~uuid.uuid5` in conjunction with
                      :func:`~socket.getfqdn` to be consistent across restarts.

    """

    def __init__(self, credentials, auth_endpoint=_DEFAULT_AUTH_ENDPOINT,
                 auth_timeout=None, queues_timeout=None, files_timeout=None,
                 queues_region=None, files_region=None,
                 queue_name='slimta-queue', container_name='slimta-queue',
                 client_id=None):
        super(RackspaceStorage, self).__init__()
        self.token_func = None
        if 'function' in credentials:
            self.token_func = credentials['function']
        elif 'username' in credentials:
            username = credentials['username']
            tenant_id = credentials.get('tenant_id')
            if 'password' in credentials:
                password = credentials['password']
                self.token_func = partial(self._get_token_password,
                                          auth_endpoint,
                                          username, password, tenant_id)
            elif 'api_key' in credentials:
                api_key = credentials['api_key']
                self.token_func = partial(self._get_token_api_key,
                                          auth_endpoint,
                                          username, api_key, tenant_id)
        if not self.token_func:
            msg = 'Required keys not found in credentials dictionary.'
            raise KeyError(msg)

        self.auth_timeout = auth_timeout
        self.queues_timeout = queues_timeout
        self.files_timeout = files_timeout
        self.queues_region = queues_region
        self.files_region = files_region
        self.queue_name = queue_name
        self.container_name = container_name
        self.client_id = client_id or _DEFAULT_CLIENT_ID

        #: The current token in use by the mechanism. When this token expires
        #: or is ``None``, a new token will be requested automatically using
        #: the credentials.
        self.token_id = None

        #: The current Cloud Queues API endpoint in use by the mechanism. This
        #: should be populated automatically on authentication.
        self.queues_endpoint = None

        #: The current Cloud Files API endpoint in use by the mechanism. This
        #: should be populated automatically on authentication.
        self.files_endpoint = None

    def _get_connection(self, parsed_url):
        host = parsed_url.netloc or 'localhost'
        host = host.rsplit(':', 1)[0]
        port = parsed_url.port
        if self.relay.tls:
            conn = HTTPSConnection(host, port, strict=True,
                                   **self.relay.tls)
        else:
            conn = HTTPConnection(host, port, strict=True)
        return conn

    def _get_token(self, url, payload):
        parsed_url = urlsplit(url, 'http')
        conn = self._get_connection(parsed_url)
        json_payload = json.dumps(payload)
        headers = [('Host', parsed_url.hostname),
                   ('Content-Type', 'application/json'),
                   ('Content-Length', str(len(json_payload))),
                   ('Accept', 'application/json')]
        with gevent.Timeout(self.auth_timeout):
            http_log.request(conn, 'POST', parsed_url.path, headers)
            conn.putrequest('POST', parsed_url.path)
            for name, value in headers:
                conn.putheader(name, value)
            conn.endheaders(json_payload)
            res = conn.getresponse()
            status = '{0!s} {1}'.format(res.status, res.reason)
            http_log.response(conn, status, res.getheaders())
            return self._get_token_response(res)

    def _get_token_response(self, response):
        if response.status != 200:
            raise RackspaceResponseError(response)
        payload = json.load(response)
        token_id = payload['access']['token']['id']
        files_endpoint = None
        queues_endpoint = None
        for service in payload['access']['serviceCatalog']:
            if service['type'] == 'object-store':
                for endpoint in service['endpoints']:
                    if not self.files_region or \
                            endpoint['publicURL'] == self.files_region:
                        files_endpoint = endpoint['publicURL']
                        break
            if service['type'] == 'rax:queue':
                for endpoint in service['endpoints']:
                    if not self.queues_region or \
                            endpoint['publicURL'] == self.queues_region:
                        queues_endpoint = endpoint['publicURL']
                        break
        if not files_endpoint:
            raise RackspaceError("""Could not discover Cloud Files endpoint \
from service catalog.""")
        elif not queues_endpoint:
            raise RackspaceError("""Could not discover Cloud Queues endpoint \
from service catalog.""")
        return token_id, queues_endpoint, files_endpoint

    def _get_token_password(self, url, username, password, tenant_id):
        payload = {'auth': {'passwordCredentials': {'username': username,
                                                    'password': password}}}
        if tenant_id:
            payload['auth']['tenantId'] = tenant_id
        return self._get_token(url, payload)

    def _get_token_api_key(self, url, username, api_key, tenant_id):
        payload = {'auth': {'RAX-KSKEY:apiKeyCredentials':
                            {'username': username, 'apiKey': api_key}}}
        if tenant_id:
            payload['auth']['tenantId'] = tenant_id
        return self._get_token(url, payload)

    def _request_token(self):
        self.token_id, self.queues_endpoint, self.files_endpoint = \
            self.token_func()

    def _get_files_url(self, files_id=None):
        url = '{0}/{1}'.format(self.files_endpoint, self.container_name)
        if files_id:
            url += '/{0}'.format(files_id)
        return url

    def _write_message(self, envelope, timestamp):
        envelope_raw = cPickle.dumps(envelope, cPickle.HIGHEST_PROTOCOL)
        files_id = str(uuid.uuid4())
        url = self._get_files_url(files_id)
        parsed_url = urlsplit(url, 'http')
        conn = self._get_connection(parsed_url)
        headers = [('Host', parsed_url.hostname),
                   ('Content-Length', str(len(envelope_raw))),
                   ('X-Object-Meta-Timestamp', json.dumps(timestamp)),
                   ('X-Object-Meta-Attempts', '0'),
                   ('X-Auth-Token', self.token_id)]
        with gevent.Timeout(self.files_timeout):
            http_log.request(conn, 'PUT', parsed_url.path, headers)
            conn.putrequest('PUT', parsed_url.path)
            for name, value in headers:
                conn.putheader(name, value)
            conn.endheaders(envelope_raw)
            res = conn.getresponse()
            status = '{0!s} {1}'.format(res.status, res.reason)
            http_log.response(conn, status, res.getheaders())
            if res.status != 201:
                raise RackspaceResponseError(res)
            return files_id

    def _write_message_meta(self, files_id, meta_headers):
        url = self._get_files_url(files_id)
        parsed_url = urlsplit(url, 'http')
        conn = self._get_connection(parsed_url)
        headers = [('Host', parsed_url.hostname),
                   ('X-Auth-Token', self.token_id)] + meta_headers
        with gevent.Timeout(self.files_timeout):
            http_log.request(conn, 'POST', parsed_url.path, headers)
            conn.putrequest('POST', parsed_url.path)
            for name, value in headers:
                conn.putheader(name, value)
            conn.endheaders()
            res = conn.getresponse()
            status = '{0!s} {1}'.format(res.status, res.reason)
            http_log.response(conn, status, res.getheaders())
            if res.status != 202:
                raise RackspaceResponseError(res)

    def _delete_message(self, files_id):
        url = self._get_files_url(files_id)
        parsed_url = urlsplit(url, 'http')
        conn = self._get_connection(parsed_url)
        headers = [('Host', parsed_url.hostname),
                   ('X-Auth-Token', self.token_id)]
        with gevent.Timeout(self.files_timeout):
            http_log.request(conn, 'DELETE', parsed_url.path, headers)
            conn.putrequest('DELETE', parsed_url.path)
            for name, value in headers:
                conn.putheader(name, value)
            conn.endheaders()
            res = conn.getresponse()
            status = '{0!s} {1}'.format(res.status, res.reason)
            http_log.response(conn, status, res.getheaders())
            if res.status != 204:
                raise RackspaceResponseError(res)

    def _get_message(self, files_id, only_meta=False):
        url = self._get_files_url(files_id)
        parsed_url = urlsplit(url, 'http')
        conn = self._get_connection(parsed_url)
        headers = [('Host', parsed_url.hostname),
                   ('Accept', 'message/rfc822'),
                   ('X-Auth-Token', self.token_id)]
        method = 'HEAD' if only_meta else 'GET'
        with gevent.Timeout(self.files_timeout):
            http_log.request(conn, method, parsed_url.path, headers)
            conn.putrequest(method, parsed_url.path)
            for name, value in headers:
                conn.putheader(name, value)
            conn.endheaders()
            res = conn.getresponse()
            status = '{0!s} {1}'.format(res.status, res.reason)
            http_log.response(conn, status, res.getheaders())
            if res.status != 200:
                raise RackspaceResponseError(res)
            timestamp = json.loads(res.getheader('X-Object-Meta-Timestamp'))
            attempts = json.loads(res.getheader('X-Object-Meta-Attempts'))
            if only_meta:
                return timestamp, attempts
            else:
                envelope = cPickle.loads(res.read())
                return envelope, timestamp, attempts

    def _list_messages(self, marker=None):
        url = self._get_files_url()
        parsed_url = urlsplit(url, 'http')
        conn = self._get_connection(parsed_url)
        headers = [('Host', parsed_url.hostname),
                   ('X-Auth-Token', self.token_id)]
        query = {'limit': '1000'}
        if marker:
            query['marker'] = marker
        selector = '{0}?{1}'.format(parsed_url.path, urlencode(query))
        with gevent.Timeout(self.files_timeout):
            http_log.request(conn, 'GET', query, headers)
            conn.putrequest('GET', query)
            for name, value in headers:
                conn.putheader(name, value)
            conn.endheaders()
            res = conn.getresponse()
            status = '{0!s} {1}'.format(res.status, res.reason)
            http_log.response(conn, status, res.getheaders())
            if res.status not in (200, 204):
                raise RackspaceResponseError(res)
            return res.read().splitlines()

    def _queue_message(self, envelope, timestamp, files_id):
        url = '{0}/queues/{1}/messages'.format(self.queues_endpoint,
                                               self.queue_name)
        parsed_url = urlsplit(url, 'http')
        conn = self._get_connection(parsed_url)
        payload = [{'ttl': 86400,
                    'body': {'timestamp', timestamp,
                             'cloud_files_id': files_id}}]
        json_payload = json.dumps(payload)
        headers = [('Host', parsed_url.hostname),
                   ('Client-ID', self.client_id),
                   ('Content-Type', 'application/json'),
                   ('Content-Length', str(len(json_payload))),
                   ('Accept', 'application/json'),
                   ('X-Auth-Token', self.token_id)]
        with gevent.Timeout(self.queues_timeout):
            http_log.request(conn, 'POST', parsed_url.path, headers)
            conn.putrequest('POST', parsed_url.path)
            for name, value in headers:
                conn.putheader(name, value)
            conn.endheaders(json_payload)
            res = conn.getresponse()
            status = '{0!s} {1}'.format(res.status, res.reason)
            http_log.response(conn, status, res.getheaders())
            if res.status != 201:
                raise RackspaceResponseError(res)

    def _claim_queued_messages(self):
        url = '{0}/queues/{1}/claims'.format(self.queues_endpoint,
                                             self.queues_name)
        parsed_url = urlsplit(url, 'http')
        conn = self._get_connection(parsed_url)
        json_payload = '{"ttl": 3600, "grace": 3600}'
        headers = [('Host', parsed_url.hostname),
                   ('Client-ID', self.client_id),
                   ('Content-Type', 'application/json'),
                   ('Content-Length', str(len(json_payload))),
                   ('Accept', 'application/json'),
                   ('X-Auth-Token', self.token_id)]
        with gevent.Timeout(self.queues_timeout):
            http_log.request(conn, 'POST', parsed_url.path, headers)
            conn.putrequest('POST', parsed_url.path)
            for name, value in headers:
                conn.putheader(name, value)
            conn.endheaders(json_payload)
            res = conn.getresponse()
            status = '{0!s} {1}'.format(res.status, res.reason)
            http_log.response(conn, status, res.getheaders())
            if res.status != 201:
                raise RackspaceResponseError(res)
            messages = json.load(res)
            return [(msg['body'], msg['href']) for msg in messages]

    def _delete_queued_message(self, href):
        url = self.queues_endpoint
        parsed_url = urlsplit(url, 'http')
        conn = self._get_connection(parsed_url)
        headers = [('Host', parsed_url.hostname),
                   ('Client-ID', self.client_id),
                   ('Content-Type', 'application/json'),
                   ('Accept', 'application/json'),
                   ('X-Auth-Token', self.token_id)]
        with gevent.Timeout(self.queues_timeout):
            http_log.request(conn, 'DELETE', href, headers)
            conn.putrequest('DELETE', href)
            for name, value in headers:
                conn.putheader(name, value)
            conn.endheaders(json_payload)
            res = conn.getresponse()
            status = '{0!s} {1}'.format(res.status, res.reason)
            http_log.response(conn, status, res.getheaders())
            if res.status != 204:
                raise RackspaceResponseError(res)

    def write(self, envelope, timestamp):
        if not self.token_id:
            self._request_token()
        files_id = self._write_contents(envelope, timestamp)
        try:
            self._queue_message(files_id)
        except Exception:
            logging.log_exception()
        return files_id

    def set_timestamp(self, id, timestamp):
        meta_headers = [('X-Object-Meta-Timestamp', json.dumps(timestamp))]
        self._write_message_meta(id, meta_headers)
        log.update_meta(id, timestamp=timestamp)

    def increment_attempts(self, id):
        timestamp, attempts = self._get_message(id, only_meta=True)
        new_attempts = attempts + 1
        meta_headers = [('X-Object-Meta-Attempts', json.dumps(new_attempts))]
        self._write_message_meta(id, meta_headers)
        log.update_meta(id, attempts=new_attempts)
        return new_attempts

    def load(self):
        marker = None
        ids = []
        while True:
            ids_batch = self._list_messages(marker)
            try:
                marker = ids_batch[-1]
            except KeyError:
                break
            ids.extend(ids_batch)
        for id in ids:
            timestamp, attempts = self._get_message(id, only_meta=True)
            yield timestamp, id

    def get(self, id):
        envelope, timestamp, attempts = self._get_message(id)
        return envelope, attempts

    def remove(self, id):
        self._delete_message(id)

    def wait(self):
        for body, href in self._claim_queued_messages():
            yield (body['timestamp'], body['cloud_files_id'])
            self._delete_queued_message(href)


# vim:et:fdm=marker:sts=4:sw=4:ts=4
