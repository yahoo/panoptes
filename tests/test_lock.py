"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""

import threading
from time import sleep

from kazoo.client import KazooState
from kazoo.testing import KazooTestCase
from mock import patch

from yahoo_panoptes.framework.context import PanoptesContext
from yahoo_panoptes.framework.utilities.lock import PanoptesLock


class TestPanoptesLock(KazooTestCase):
    @staticmethod
    def make_event():
        return threading.Event()

    def mock_kazoo_client(self):
        return self.client

    def test_panoptes_lock(self):
        connected = threading.Event()
        lost_connection = threading.Event()

        def _listener(state):
            if state == KazooState.CONNECTED:
                connected.set()
            else:
                lost_connection.set()

        self.client.add_listener(_listener)

        with patch.object(PanoptesContext, u'_get_zookeeper_client', self.mock_kazoo_client):
            panoptes_context = PanoptesContext(config_file=u'tests/config_files/test_panoptes_config.ini',
                                               create_zookeeper_client=True)

            # Test that bad parameters fail
            with self.assertRaises(AssertionError):
                PanoptesLock(context=None, path=u'/lock', timeout=5, retries=0, identifier=u'test')
            with self.assertRaises(AssertionError):
                PanoptesLock(context=panoptes_context, path=u'/lock', timeout=5, retries=None, identifier=u'test')
            with self.assertRaises(AssertionError):
                PanoptesLock(context=panoptes_context, path=u'/lock', timeout=5, retries=-1, identifier=u'test')
            with self.assertRaises(AssertionError):
                PanoptesLock(context=panoptes_context, path=u'/lock', timeout=5, retries='1', identifier=u'test')
            with self.assertRaises(AssertionError):
                PanoptesLock(context=panoptes_context, path=None, timeout=5, retries=0, identifier=u'test')
            with self.assertRaises(AssertionError):
                PanoptesLock(context=panoptes_context, path=u'lock', timeout=5, retries=0, identifier=u'test')
            with self.assertRaises(AssertionError):
                PanoptesLock(context=panoptes_context, path=u'/lock', timeout=None, retries=0, identifier=u'test')
            with self.assertRaises(AssertionError):
                PanoptesLock(context=panoptes_context, path=u'/lock', timeout=-1, retries=0, identifier=u'test')
            with self.assertRaises(AssertionError):
                PanoptesLock(context=panoptes_context, path=u'/lock', timeout=-1, retries=0, identifier=u'test')
            with self.assertRaises(AssertionError):
                PanoptesLock(context=panoptes_context, path=u'/lock', timeout=5, retries=0, identifier=None)
            with self.assertRaises(AssertionError):
                PanoptesLock(context=panoptes_context, path=u'/lock', timeout=5, retries=0, identifier=1)

            # Acquire lock with unlimited retries
            lock = PanoptesLock(context=panoptes_context, path=u'/lock', timeout=5, retries=0, identifier=u'test')
            self.assertEquals(lock.locked, True)

            # Release the lock
            lock.release()
            self.assertEquals(lock.locked, False)

            # Acquire lock with only one retry
            lock1 = PanoptesLock(context=panoptes_context, path=u'/lock', timeout=5, identifier=u'test')
            self.assertEquals(lock1.locked, True)

            # Try an acquire an acquired lock - this should fail
            lock2 = PanoptesLock(context=panoptes_context, path=u'/lock', timeout=5, identifier=u'test')
            self.assertEquals(lock2.locked, False)
            lock1.release()
            self.assertEquals(lock1.locked, False)
            lock2.release()

            # Acquire the lock, lose connection and lose the lock and acquire it again on reconnection
            lock = PanoptesLock(context=panoptes_context, path=u'/lock', timeout=5, identifier=u'test')
            self.assertEquals(lock.locked, True)
            self.lose_connection(self.make_event)
            # Block till the client disconnects - or 30 seconds pass
            lost_connection.wait(30)
            # Verify that the client actually lost the connection
            self.assertEquals(lost_connection.is_set(), True)
            # Give it time to cleanup the lock
            # TODO: There is a timing issue here - if we sleep too long before checking the state of the lock, we
            # might get reconnected and reacquire the lock, which is why we check if connected.is_set is NOT set
            # before the
            # assert
            sleep(0.1)
            # The lock should be not be set after we loose a connection
            if not connected.is_set():
                self.assertEquals(lock.locked, False)
            # Block till the client reconnects - or 30 seconds pass
            connected.wait(30)
            # Verify that the client actually reconnected
            self.assertEquals(connected.is_set(), True)
            # Give it time to reacquire the lock
            sleep(3)
            self.assertEquals(lock.locked, True)
