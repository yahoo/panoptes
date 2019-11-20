"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
import os
import unittest

from celery.beat import Service
from mock import create_autospec, patch, MagicMock

from yahoo_panoptes.polling.polling_plugin_scheduler import polling_plugin_scheduler_task, \
    start_polling_plugin_scheduler

from yahoo_panoptes.framework.celery_manager import PanoptesCeleryConfig, PanoptesCeleryPluginScheduler
from yahoo_panoptes.framework.resources import PanoptesContext, PanoptesResource
from yahoo_panoptes.framework.plugins.scheduler import PanoptesPluginScheduler

from tests.test_framework import PanoptesTestKeyValueStore, panoptes_mock_kazoo_client, \
    panoptes_mock_redis_strict_client


def get_test_conf_file():
    my_dir = os.path.dirname(os.path.realpath(__file__))
    panoptes_test_conf_file = os.path.join(my_dir, u'../config_files/test_panoptes_config_polling_plugin_scheduler.ini')
    return my_dir, panoptes_test_conf_file


def _callback(*args):
    pass


def mock_get_resources(*args):
    mock_resource = create_autospec(PanoptesResource)
    return [mock_resource]


class TestPanoptesPollingPluginScheduler(unittest.TestCase):
    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = get_test_conf_file()
        self._panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                                 key_value_store_class_list=[PanoptesTestKeyValueStore],
                                                 create_message_producer=False,
                                                 create_zookeeper_client=True)
        self._celery_config = PanoptesCeleryConfig(app_name=u"Polling Plugin Test")
        self._scheduler = PanoptesPluginScheduler(
            panoptes_context=self._panoptes_context,
            plugin_type=u"polling",
            plugin_type_display_name=u"Polling",
            celery_config=self._celery_config,
            lock_timeout=1,
            plugin_scheduler_task=_callback
        )

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_basic_operations(self):
        celery_app = self._scheduler.start()
        celery_beat_service = Service(celery_app, max_interval=None, schedule_filename=None,
                                      scheduler_cls=PanoptesCeleryPluginScheduler)
        with patch('yahoo_panoptes.polling.polling_plugin_scheduler.const.DEFAULT_CONFIG_FILE_PATH',
                   self.panoptes_test_conf_file):
            with patch('yahoo_panoptes.polling.polling_plugin_scheduler.PanoptesResourceCache.get_resources',
                       mock_get_resources):
                start_polling_plugin_scheduler()
                polling_plugin_scheduler_task(celery_beat_service)

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_error_messages(self):
        celery_app = self._scheduler.start()
        celery_beat_service = Service(celery_app, max_interval=None, schedule_filename=None,
                                      scheduler_cls=PanoptesCeleryPluginScheduler)
        with patch('yahoo_panoptes.polling.polling_plugin_scheduler.const.DEFAULT_CONFIG_FILE_PATH',
                   self.panoptes_test_conf_file):

            mock_plugin_manager = MagicMock(side_effect=Exception)
            with patch('yahoo_panoptes.polling.polling_plugin_scheduler.PanoptesPluginManager',
                       mock_plugin_manager):
                start_polling_plugin_scheduler()
                polling_plugin_scheduler_task(celery_beat_service)

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_polling_plugin_scheduler_task_bad_plugin(self):
        celery_app = self._scheduler.start()
        celery_beat_service = Service(celery_app, max_interval=None, schedule_filename=None,
                                      scheduler_cls=PanoptesCeleryPluginScheduler)
        with patch('yahoo_panoptes.polling.polling_plugin_scheduler.const.DEFAULT_CONFIG_FILE_PATH',
                   self.panoptes_test_conf_file):
            with patch('yahoo_panoptes.polling.polling_plugin_scheduler.PanoptesPollingPluginInfo.'
                       'execute_frequency', 0):
                start_polling_plugin_scheduler()
                polling_plugin_scheduler_task(celery_beat_service)
            with patch('yahoo_panoptes.polling.polling_plugin_scheduler.PanoptesPollingPluginInfo.'
                       'resource_filter', None):
                start_polling_plugin_scheduler()
                polling_plugin_scheduler_task(celery_beat_service)

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_polling_plugin_scheduler_task_config_error(self):
        celery_app = self._scheduler.start()
        celery_beat_service = Service(celery_app, max_interval=None, schedule_filename=None,
                                      scheduler_cls=PanoptesCeleryPluginScheduler)
        with patch('yahoo_panoptes.polling.polling_plugin_scheduler.const.DEFAULT_CONFIG_FILE_PATH',
                   self.panoptes_test_conf_file):
            mock_getmtime = MagicMock(side_effect=Exception)
            with patch('yahoo_panoptes.framework.plugins.panoptes_base_plugin.os.path.getmtime', mock_getmtime):
                start_polling_plugin_scheduler()
                polling_plugin_scheduler_task(celery_beat_service)

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_resource_cache_error(self):
        celery_app = self._scheduler.start()
        celery_beat_service = Service(celery_app, max_interval=None, schedule_filename=None,
                                      scheduler_cls=PanoptesCeleryPluginScheduler)
        with patch('yahoo_panoptes.polling.polling_plugin_scheduler.const.DEFAULT_CONFIG_FILE_PATH',
                   self.panoptes_test_conf_file):
            mock_cache = MagicMock(side_effect=Exception)
            with patch('yahoo_panoptes.polling.polling_plugin_scheduler.PanoptesResourceCache', mock_cache):
                start_polling_plugin_scheduler()
                polling_plugin_scheduler_task(celery_beat_service)

            mock_get_resources_exception = MagicMock(side_effect=Exception)
            with patch('yahoo_panoptes.polling.polling_plugin_scheduler.PanoptesResourceCache.get_resources',
                       mock_get_resources_exception):
                start_polling_plugin_scheduler()
                polling_plugin_scheduler_task(celery_beat_service)

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_polling_plugin_scheduler_update_error(self):
        celery_app = self._scheduler.start()
        celery_beat_service = Service(celery_app, max_interval=None, schedule_filename=None,
                                      scheduler_cls=PanoptesCeleryPluginScheduler)
        with patch('yahoo_panoptes.polling.polling_plugin_scheduler.const.DEFAULT_CONFIG_FILE_PATH',
                   self.panoptes_test_conf_file):
            mock_update = MagicMock(side_effect=Exception)
            with patch('yahoo_panoptes.framework.celery_manager.PanoptesCeleryPluginScheduler.update', mock_update):
                start_polling_plugin_scheduler()
                polling_plugin_scheduler_task(celery_beat_service)

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_polling_plugin_scheduler_context_error(self):
        with patch('yahoo_panoptes.polling.polling_plugin_scheduler.const.DEFAULT_CONFIG_FILE_PATH',
                   self.panoptes_test_conf_file):
            mock_context = MagicMock(side_effect=Exception)
            with patch('yahoo_panoptes.polling.polling_plugin_scheduler.PanoptesPollingPluginSchedulerContext',
                       mock_context):
                with self.assertRaises(SystemExit):
                    start_polling_plugin_scheduler()

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_polling_plugin_scheduler_agent_config_error(self):
        with patch('yahoo_panoptes.polling.polling_plugin_scheduler.const.DEFAULT_CONFIG_FILE_PATH',
                   self.panoptes_test_conf_file):
            mock_config = MagicMock(side_effect=Exception)
            with patch('yahoo_panoptes.polling.polling_plugin_scheduler.PanoptesCeleryPollingAgentConfig',
                       mock_config):
                with self.assertRaises(SystemExit):
                    start_polling_plugin_scheduler()

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_plugin_scheduler_error(self):
        with patch('yahoo_panoptes.polling.polling_plugin_scheduler.const.DEFAULT_CONFIG_FILE_PATH',
                   self.panoptes_test_conf_file):
            mock_scheduler = create_autospec(PanoptesPluginScheduler, side_effect=Exception)
            with patch('yahoo_panoptes.polling.polling_plugin_scheduler.PanoptesPluginScheduler',
                       mock_scheduler):
                with self.assertRaises(SystemExit):
                    start_polling_plugin_scheduler()

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_celery_errors(self):
        with patch('yahoo_panoptes.polling.polling_plugin_scheduler.const.DEFAULT_CONFIG_FILE_PATH',
                   self.panoptes_test_conf_file):
            mock_start = create_autospec(PanoptesPluginScheduler.start, return_value=None)
            with patch('yahoo_panoptes.polling.polling_plugin_scheduler.PanoptesPluginScheduler.start',
                       mock_start):
                with self.assertRaises(SystemExit):
                    start_polling_plugin_scheduler()

            mock_start.side_effect = Exception
            with patch('yahoo_panoptes.polling.polling_plugin_scheduler.PanoptesPluginScheduler.start',
                       mock_start):
                with self.assertRaises(SystemExit):
                    start_polling_plugin_scheduler()
