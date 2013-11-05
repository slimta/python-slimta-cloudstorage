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

from functools import partial

import pyrax

from slimta.queue import QueueStorage
from slimta import logging

__all__ = ['RackspaceStorage']

log = logging.getQueueStorageLogger(__name__)


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
                        key.
    :type credentials: dict
    :param auth_endpoint: If given, this is the Rackspace Cloud Auth endpoint to
                          hit when creating tokens.

    """

    def __init__(self, credentials, auth_endpoint=None):
        super(RackspaceStorage, self).__init__()
        self.get_token = None
        if 'function' in credentials:
            self.get_token = credentials['function']
        elif 'username' in credentials:
            username = credentials['username']
            if 'password' in credentials:
                password = credentials['password']
                self.get_token = partial(self._get_token_password,
                                         username, password)
            elif 'api_key' in credentials:
                api_key = credentials['api_key']
                self.get_token = partial(self._get_token_api_key,
                                         username, api_key)
        if not self.get_token:
            raise KeyError('Required keys not found in credentials dictionary.')

    def _get_token_password(self, username, password):
        pass

    def _get_token_api_key(self, username, api_key):
        pass


# vim:et:fdm=marker:sts=4:sw=4:ts=4
