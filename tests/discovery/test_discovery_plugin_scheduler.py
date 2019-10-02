"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
import unittest
import signal

from celery import app
from mock import patch, MagicMock

from celery.beat import Service

from yahoo_panoptes.discovery.discovery_plugin_scheduler import PanoptesDiscoveryPluginSchedulerContext, \
    PanoptesDiscoveryPluginSchedulerKeyValueStore, discovery_plugin_scheduler_task, start_discovery_plugin_scheduler, \
    celery_beat_service_started
from yahoo_panoptes.framework.celery_manager import PanoptesCeleryConfig, PanoptesCeleryPluginScheduler
from yahoo_panoptes.framework.resources import PanoptesContext
from yahoo_panoptes.framework.plugins.scheduler import PanoptesPluginScheduler

from tests.test_framework import PanoptesTestKeyValueStore, panoptes_mock_kazoo_client, \
    panoptes_mock_redis_strict_client
from tests.helpers import get_test_conf_file


def _callback(*args):
    pass


class TestPanoptesDiscoveryPluginScheduler(unittest.TestCase):
    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = get_test_conf_file()
        self._panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                                 key_value_store_class_list=[PanoptesTestKeyValueStore],
                                                 create_message_producer=False, async_message_producer=False,
                                                 create_zookeeper_client=True)
        self._celery_config = PanoptesCeleryConfig(app_name="Discovery Plugin Test")
        # self._scheduler = PanoptesPluginScheduler(
        #     panoptes_context=self._panoptes_context,
        #     plugin_type="polling",
        #     plugin_type_display_name="Polling",
        #     celery_config=self._celery_config,
        #     lock_timeout=1,
        #     plugin_scheduler_task=_callback
        # )

    def test_basic_operations(self):
        with patch('yahoo_panoptes.discovery.discovery_plugin_scheduler.const.DEFAULT_CONFIG_FILE_PATH',
                   self.panoptes_test_conf_file):
            celery_app = start_discovery_plugin_scheduler()
