"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
import unittest

from celery import app
from mock import patch, MagicMock

from yahoo_panoptes.framework.celery_manager import PanoptesCeleryConfig
from yahoo_panoptes.framework.resources import PanoptesContext
from yahoo_panoptes.framework.plugins.scheduler import PanoptesPluginScheduler

from .test_framework import PanoptesTestKeyValueStore, panoptes_mock_kazoo_client, panoptes_mock_redis_strict_client
from .helpers import get_test_conf_file


def _callback(*args):
    pass


def _callback_no_args():
    pass


class TestPanoptesPluginScheduler(unittest.TestCase):
    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = get_test_conf_file()
        self._panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                                 key_value_store_class_list=[PanoptesTestKeyValueStore],
                                                 create_message_producer=False, async_message_producer=False,
                                                 create_zookeeper_client=True)
        self._celery_config = PanoptesCeleryConfig(app_name="Polling Plugin Test")
        self._scheduler = PanoptesPluginScheduler(self._panoptes_context, "polling", "Polling", self._celery_config, 1,
                                                  _callback)

    def test_start_basic_operations(self):
        # Test bad input
        with self.assertRaises(AssertionError):
            PanoptesPluginScheduler("test", "polling", "Polling", self._celery_config, 1,
                                    _callback)
        with self.assertRaises(AssertionError):
            PanoptesPluginScheduler(self._panoptes_context, "", "Polling",self._celery_config, 1,
                                    _callback)
        with self.assertRaises(AssertionError):
            PanoptesPluginScheduler(self._panoptes_context, "polling", "",self._celery_config, 1,
                                    _callback)
        with self.assertRaises(AssertionError):
            PanoptesPluginScheduler(self._panoptes_context, "polling", "Polling", "Test", 1,
                                    _callback)
        with self.assertRaises(AssertionError):
            PanoptesPluginScheduler(self._panoptes_context, "polling", "Polling",self._celery_config, 0,
                                    _callback)
        with self.assertRaises(AssertionError):
            PanoptesPluginScheduler(self._panoptes_context, "polling", "Polling", "Test", 1,
                                    object)

        # Test locking error when starting up the scheduler
        mock_lock = MagicMock(side_effect=Exception)
        with patch('yahoo_panoptes.framework.plugins.scheduler.PanoptesLock', mock_lock):
            with self.assertRaises(SystemExit):
                self._scheduler.start()

        celery_app = self._scheduler.start()
        self.assertIsInstance(celery_app, app.base.Celery)

    def test_celery_beat_error(self):
        mock_celery_instance = MagicMock(side_effect=Exception)
        with patch('yahoo_panoptes.framework.plugins.scheduler.PanoptesCeleryInstance', mock_celery_instance):
            celery_app = self._scheduler.start()
            self.assertIsNone(celery_app)
