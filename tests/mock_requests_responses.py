"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""


class MockRequestsResponseBadRequest(object):
    status_code = 400
    content = 'Bad Request'


class MockRequestsResponseServerFailure(object):
    status_code = 500
    content = 'Internal Server Failure'


class MockRequestsResponseOK(object):
    status_code = 200
    content = 'OK'
