"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
from builtins import object
import re

from kazoo.client import KazooState
from kazoo.exceptions import LockTimeout

from yahoo_panoptes.framework.validators import PanoptesValidators
from yahoo_panoptes.framework.context import PanoptesContextValidators
from yahoo_panoptes.framework.utilities.helpers import get_calling_module_name


class PanoptesLock(object):
    def __init__(self, context, path, timeout, retries=1, identifier=None):
        """
        Creates and maintains state for a lock

        Args:
            path (str): A '/' separated path for the lock
            timeout (int): in seconds. Must be a positive integer
            retries (int): how many times to try before giving up. Zero implies try forever
            identifier (str): Name to use for this lock contender. This can be useful for querying \
            to see who the current lock contenders are

        Returns:
            PanoptesLock: lock
        """
        assert PanoptesContextValidators.valid_panoptes_context(context), u'context must be a valid PanoptesContext'
        assert PanoptesValidators.valid_nonempty_string(path) and re.search(r"^/\S+", path), \
            u'path must be a non-empty string that begins with /'
        assert PanoptesValidators.valid_nonzero_integer(timeout), u'timeout must be a positive integer'
        assert PanoptesValidators.valid_positive_integer(retries), u'retries must be a non-negative integer'
        assert PanoptesValidators.valid_nonempty_string(identifier), u'identifier must be a non-empty string'

        self._context = context
        self._logger = self._context.logger
        self._path = path
        self._timeout = timeout
        self._retries = retries
        self._identifier = identifier
        self._lock = None
        self._locked = False
        self._calling_module = get_calling_module_name(3)

        self._get_lock()

    def __str__(self):
        return u'calling module: {}, path={}, timeout={}, retries={}, identifier={}'.format(
                self._calling_module, self._path, self._timeout, self._retries, self._identifier)

    @property
    def locked(self):
        return self._locked

    def release(self):
        if self._lock:
            self._logger.info(u'Releasing lock for {}'.format(str(self)))
            self._lock.release()

        self._locked = False
        self._lock = None

    def _get_lock(self):
        """
        A wrapper around the Kazoo library's lock. On successful acquisition of the lock, sets self._lock and
        self._locked

        Returns:
            None
        """
        logger = self._logger

        logger.info(u'Creating lock for {}'.format(str(self)))

        try:
            lock = self._context.zookeeper_client.Lock(self._path, self._identifier)
        except:
            logger.exception(u'Failed to create lock object')
            return

        tries = 0
        while (self._retries == 0) or (tries < self._retries):
            tries += 1
            logger.info(u'Trying to acquire lock for {}. Other contenders: {}'.format(str(self), lock.contenders()))
            try:
                lock.acquire(timeout=self._timeout)
            except LockTimeout:
                logger.info(u'Timed out after {} seconds trying to acquire lock for {}'.format(self._timeout,
                                                                                               str(self)))
            except Exception as e:
                logger.info(u'Error in acquiring lock for {}: {}'.format(str(self), repr(e)))

            if lock.is_acquired:
                break

        if not lock.is_acquired:
            logger.warn(u'Failed to acquire lock for {} after {} tries'.format(str(self), tries))
        else:
            logger.info(u'Lock acquired for {}. Other contenders: {}'.format(str(self), lock.contenders()))
            self._locked = True
            self._context.zookeeper_client.add_listener(self._lock_listener)
            self._lock = lock

    def _release_and_reacquire(self):
        if self._lock:
            self.release()
            self._get_lock()

    def _lock_listener(self, state):
        """
        Listener to handle ZK disconnection/reconnection. Since I don't know of safe way to check if a lock is still in
        ZK after a reconnect, we simply release the lock and try and re-acquire it.

        Args:
            state (kazoo.client.KazooState): The state of the ZK connection

        Returns:
            None
        """

        if state in [KazooState.LOST, KazooState.SUSPENDED]:
            self._logger.warn(u'Disconnected from Zookeeper, waiting to reconnect lock for {}'.format(str(self)))
            self._locked = False
        elif state == KazooState.CONNECTED:
            self._logger.warn(
                    u'Reconnected to Zookeeper, trying to release and re-acquire lock for {}'.format(str(self)))
            self._context.zookeeper_client.handler.spawn(self._release_and_reacquire)
        else:
            self._logger.warn(u'Got unknown state "{}" from Zookeeper'.format(state))
