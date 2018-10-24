"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
import os
import signal
import time
import unittest

from celery import app
from mock import patch, MagicMock

from yahoo_panoptes.framework.plugins.panoptes_base_plugin import PanoptesPluginInfo, PanoptesBasePlugin
from yahoo_panoptes.polling.polling_plugin import PanoptesPollingPlugin
from yahoo_panoptes.framework.celery_manager import PanoptesCeleryConfig, PanoptesCeleryInstance
from yahoo_panoptes.framework.resources import PanoptesContext, PanoptesResource
from yahoo_panoptes.framework.plugins.scheduler import PanoptesPluginScheduler
from yahoo_panoptes.framework.metrics import PanoptesMetric, PanoptesMetricsGroupSet

from .test_framework import PanoptesTestKeyValueStore, panoptes_mock_kazoo_client, panoptes_mock_redis_strict_client


def _callback(*args):
    print "##### _callback called."


def _callback_no_args():
    pass


def mock_wait(*args):
    print "#### waiting..."


def basic_operations():
    pass


class TestPanoptesPluginScheduler(unittest.TestCase):
    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = _get_test_conf_file()
        self._panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                                 key_value_store_class_list=[PanoptesTestKeyValueStore],
                                                 create_message_producer=False, async_message_producer=False,
                                                 create_zookeeper_client=True)

    def test_basic_operations(self):
        # mock_wait = MagicMock()
        with patch('yahoo_panoptes.framework.plugins.scheduler.threading._Event.wait',
                   mock_wait):
            celery_config = PanoptesCeleryConfig(app_name="Polling Plugin Test")
            scheduler = PanoptesPluginScheduler(self._panoptes_context, "polling", "Polling", celery_config, 1,
                                                _callback)

            celery_app = scheduler.start()
            print "#### type: %s" % type(celery_app)
            self.assertIsInstance(celery_app, app.base.Celery)
            print "#### os.getpid(): %s" % os.getpid()
            scheduler.run()

            # TODO kill plugin_scheduler_task_thread
            # os.kill(os.getpid(), signal.SIGUSR1)


def _get_test_conf_file():
    my_dir = os.path.dirname(os.path.realpath(__file__))
    panoptes_test_conf_file = os.path.join(my_dir, 'config_files/test_panoptes_config.ini')

    return my_dir, panoptes_test_conf_file
