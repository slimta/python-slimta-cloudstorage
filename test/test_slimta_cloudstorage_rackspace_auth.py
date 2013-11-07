
import json

from mox import MoxTestBase, IsA, Func

from slimta.cloudstorage.rackspace import RackspaceCloudAuth


class TestRackspaceCloudAuth(MoxTestBase):

    def setUp(self):
        super(TestRackspaceCloudAuth, self).setUp()
        self.response_payload = {'access': {
                'token': {'id': 'tokenid'},
                'serviceCatalog': [
                        {'type': 'object-store',
                         'endpoints': [
                                {'region': 'TEST',
                                 'publicURL': 'http://files/v1'},
                                {'region': 'OTHER',
                                 'publicURL': 'http://files-other/v1'}
                             ]},
                        {'type': 'rax:queues',
                         'endpoints': [
                                {'region': 'TEST',
                                 'publicURL': 'http://queues/v1'},
                                {'region': 'OTHER',
                                 'publicURL': 'http://queues-other/v1'}
                             ]},
                    ],
            }}

    def test_create_token_func(self):
        func = self.mox.CreateMockAnything()
        func.__call__().AndReturn(('tokenid', 'files', 'queues'))
        self.mox.ReplayAll()
        auth = RackspaceCloudAuth({'function': func})
        self.assertEqual('tokenid', auth.token_id)
        self.assertEqual('files', auth.files_endpoint)
        self.assertEqual('queues', auth.queues_endpoint)

    def test_create_token_password(self):
        auth = RackspaceCloudAuth({'username': 'testuser', 'password': 'testpass'}, 'http://test/v1', 'TEST')
        conn = self.mox.CreateMockAnything()
        res = self.mox.CreateMockAnything()
        self.mox.StubOutWithMock(auth, 'get_connection')
        auth.get_connection(IsA(tuple), {}).AndReturn(conn)
        conn.putrequest('POST', '/v1/tokens')
        conn.putheader('Host', 'test')
        conn.putheader('Content-Type', 'application/json')
        conn.putheader('Content-Length', '83')
        conn.putheader('Accept', 'application/json')
        conn.endheaders('{"auth": {"passwordCredentials": {"username": "testuser", "password": "testpass"}}}')
        res.status = 200
        res.reason = 'OK'
        conn.getresponse().AndReturn(res)
        res.getheaders().AndReturn([])
        res.read().AndReturn(json.dumps(self.response_payload))
        self.mox.ReplayAll()
        self.assertEqual('tokenid', auth.token_id)
        self.assertEqual('http://files/v1', auth.files_endpoint)
        self.assertEqual('http://queues/v1', auth.queues_endpoint)

    def test_create_token_api_key(self):
        auth = RackspaceCloudAuth({'username': 'testuser', 'api_key': 'testkey'}, 'http://test/v1', 'TEST')
        conn = self.mox.CreateMockAnything()
        res = self.mox.CreateMockAnything()
        self.mox.StubOutWithMock(auth, 'get_connection')
        auth.get_connection(IsA(tuple), {}).AndReturn(conn)
        conn.putrequest('POST', '/v1/tokens')
        conn.putheader('Host', 'test')
        conn.putheader('Content-Type', 'application/json')
        conn.putheader('Content-Length', '88')
        conn.putheader('Accept', 'application/json')
        conn.endheaders('{"auth": {"RAX-KSKEY:apiKeyCredentials": {"username": "testuser", "apiKey": "testkey"}}}')
        res.status = 200
        res.reason = 'OK'
        conn.getresponse().AndReturn(res)
        res.getheaders().AndReturn([])
        res.read().AndReturn(json.dumps(self.response_payload))
        self.mox.ReplayAll()
        self.assertEqual('tokenid', auth.token_id)
        self.assertEqual('http://files/v1', auth.files_endpoint)
        self.assertEqual('http://queues/v1', auth.queues_endpoint)


# vim:et:fdm=marker:sts=4:sw=4:ts=4
