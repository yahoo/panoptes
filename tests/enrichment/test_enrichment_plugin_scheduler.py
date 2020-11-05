"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
import unittest

from celery.beat import Service
from mock import create_autospec, patch, MagicMock

from yahoo_panoptes.enrichment.enrichment_plugin_scheduler import enrichment_plugin_scheduler_task, \
    celery_beat_service_started, start_enrichment_plugin_scheduler

from yahoo_panoptes.framework.celery_manager import PanoptesCeleryConfig, PanoptesCeleryPluginScheduler
from yahoo_panoptes.framework.resources import PanoptesContext, PanoptesResource
from yahoo_panoptes.framework.plugins.scheduler import PanoptesPluginScheduler

from tests.test_framework import PanoptesTestKeyValueStore, panoptes_mock_kazoo_client, \
    panoptes_mock_redis_strict_client
from tests.helpers import get_test_conf_file


def _callback(*args):
    """
    Call the callback function to call.

    Args:
    """
    pass


def mock_get_resources(*args):
    """
    Return a list. resources.

    Args:
    """
    mock_resource = create_autospec(PanoptesResource)
    return [mock_resource]


class TestPanoptesEnrichmentPluginScheduler(unittest.TestCase):
    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def setUp(self):
        """
        Set up the configuration.

        Args:
            self: (todo): write your description
        """
        self.my_dir, self.panoptes_test_conf_file = get_test_conf_file()
        self._panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                                 key_value_store_class_list=[PanoptesTestKeyValueStore],
                                                 create_message_producer=False, async_message_producer=False,
                                                 create_zookeeper_client=True)
        self._celery_config = PanoptesCeleryConfig(app_name=u"Enrichment Plugin Test")
        self._scheduler = PanoptesPluginScheduler(
            panoptes_context=self._panoptes_context,
            plugin_type=u"enrichment",
            plugin_type_display_name=u"Enrichment",
            celery_config=self._celery_config,
            lock_timeout=1,
            plugin_scheduler_task=_callback
        )

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_basic_operations(self):
        """
        Test the scheduler operations.

        Args:
            self: (todo): write your description
        """
        celery_app = self._scheduler.start()
        celery_beat_service = Service(celery_app, max_interval=None, schedule_filename=None,
                                      scheduler_cls=PanoptesCeleryPluginScheduler)
        with patch('yahoo_panoptes.enrichment.enrichment_plugin_scheduler.const.DEFAULT_CONFIG_FILE_PATH',
                   self.panoptes_test_conf_file):
            with patch('yahoo_panoptes.enrichment.enrichment_plugin_scheduler.PanoptesResourceCache.get_resources',
                       mock_get_resources):
                start_enrichment_plugin_scheduler()
                enrichment_plugin_scheduler_task(celery_beat_service)

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_error_messages(self):
        """
        Test for test test.

        Args:
            self: (todo): write your description
        """
        celery_app = self._scheduler.start()
        celery_beat_service = Service(celery_app, max_interval=None, schedule_filename=None,
                                      scheduler_cls=PanoptesCeleryPluginScheduler)
        with patch('yahoo_panoptes.enrichment.enrichment_plugin_scheduler.const.DEFAULT_CONFIG_FILE_PATH',
                   self.panoptes_test_conf_file):

            mock_plugin_manager = MagicMock(side_effect=Exception)
            with patch('yahoo_panoptes.enrichment.enrichment_plugin_scheduler.PanoptesPluginManager',
                       mock_plugin_manager):
                start_enrichment_plugin_scheduler()
                enrichment_plugin_scheduler_task(celery_beat_service)

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_enrichment_plugin_scheduler_task_bad_plugin(self):
        """
        Test for scheduler scheduler

        Args:
            self: (todo): write your description
        """
        celery_app = self._scheduler.start()
        celery_beat_service = Service(celery_app, max_interval=None, schedule_filename=None,
                                      scheduler_cls=PanoptesCeleryPluginScheduler)
        with patch('yahoo_panoptes.enrichment.enrichment_plugin_scheduler.const.DEFAULT_CONFIG_FILE_PATH',
                   self.panoptes_test_conf_file):
            with patch('yahoo_panoptes.enrichment.enrichment_plugin_scheduler.PanoptesEnrichmentPluginInfo.'
                       'execute_frequency', 0):
                start_enrichment_plugin_scheduler()
                enrichment_plugin_scheduler_task(celery_beat_service)
            with patch('yahoo_panoptes.enrichment.enrichment_plugin_scheduler.PanoptesEnrichmentPluginInfo.'
                       'resource_filter', None):
                start_enrichment_plugin_scheduler()
                enrichment_plugin_scheduler_task(celery_beat_service)

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_enrichment_plugin_scheduler_task_config_error(self):
        """
        Test if the scheduler configuration

        Args:
            self: (todo): write your description
        """
        celery_app = self._scheduler.start()
        celery_beat_service = Service(celery_app, max_interval=None, schedule_filename=None,
                                      scheduler_cls=PanoptesCeleryPluginScheduler)
        with patch('yahoo_panoptes.enrichment.enrichment_plugin_scheduler.const.DEFAULT_CONFIG_FILE_PATH',
                   self.panoptes_test_conf_file):
            mock_getmtime = MagicMock(side_effect=Exception)
            with patch('yahoo_panoptes.framework.plugins.panoptes_base_plugin.os.path.getmtime', mock_getmtime):
                start_enrichment_plugin_scheduler()
                enrichment_plugin_scheduler_task(celery_beat_service)

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_resource_cache_error(self):
        """
        Return a resource cache.

        Args:
            self: (todo): write your description
        """
        celery_app = self._scheduler.start()
        celery_beat_service = Service(celery_app, max_interval=None, schedule_filename=None,
                                      scheduler_cls=PanoptesCeleryPluginScheduler)
        with patch('yahoo_panoptes.enrichment.enrichment_plugin_scheduler.const.DEFAULT_CONFIG_FILE_PATH',
                   self.panoptes_test_conf_file):
            mock_cache = MagicMock(side_effect=Exception)
            with patch('yahoo_panoptes.enrichment.enrichment_plugin_scheduler.PanoptesResourceCache', mock_cache):
                start_enrichment_plugin_scheduler()
                enrichment_plugin_scheduler_task(celery_beat_service)

            mock_get_resources_exception = MagicMock(side_effect=Exception)
            with patch('yahoo_panoptes.enrichment.enrichment_plugin_scheduler.PanoptesResourceCache.get_resources',
                       mock_get_resources_exception):
                start_enrichment_plugin_scheduler()
                enrichment_plugin_scheduler_task(celery_beat_service)

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_enrichment_plugin_scheduler_update_error(self):
        """
        Test if scheduler plugin

        Args:
            self: (todo): write your description
        """
        celery_app = self._scheduler.start()
        celery_beat_service = Service(celery_app, max_interval=None, schedule_filename=None,
                                      scheduler_cls=PanoptesCeleryPluginScheduler)
        with patch('yahoo_panoptes.enrichment.enrichment_plugin_scheduler.const.DEFAULT_CONFIG_FILE_PATH',
                   self.panoptes_test_conf_file):
            mock_update = MagicMock(side_effect=Exception)
            with patch('yahoo_panoptes.framework.celery_manager.PanoptesCeleryPluginScheduler.update', mock_update):
                start_enrichment_plugin_scheduler()
                enrichment_plugin_scheduler_task(celery_beat_service)

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_enrichment_plugin_scheduler_context_error(self):
        """
        Perform plugin plugin plugin plugin plugin.

        Args:
            self: (todo): write your description
        """
        with patch('yahoo_panoptes.enrichment.enrichment_plugin_scheduler.const.DEFAULT_CONFIG_FILE_PATH',
                   self.panoptes_test_conf_file):
            mock_context = MagicMock(side_effect=Exception)
            with patch('yahoo_panoptes.enrichment.enrichment_plugin_scheduler.PanoptesEnrichmentPluginSchedulerContext',
                       mock_context):
                with self.assertRaises(SystemExit):
                    start_enrichment_plugin_scheduler()

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_enrichment_plugin_scheduler_agent_config_error(self):
        """
        Test if the configuration agent agent config plugin is sent.

        Args:
            self: (todo): write your description
        """
        with patch('yahoo_panoptes.enrichment.enrichment_plugin_scheduler.const.DEFAULT_CONFIG_FILE_PATH',
                   self.panoptes_test_conf_file):
            mock_config = MagicMock(side_effect=Exception)
            with patch('yahoo_panoptes.enrichment.enrichment_plugin_scheduler.PanoptesCeleryEnrichmentAgentConfig',
                       mock_config):
                with self.assertRaises(SystemExit):
                    start_enrichment_plugin_scheduler()

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def test_celery_none(self):
        """
        Test if the mock test_celery.

        Args:
            self: (todo): write your description
        """
        with patch('yahoo_panoptes.enrichment.enrichment_plugin_scheduler.const.DEFAULT_CONFIG_FILE_PATH',
                   self.panoptes_test_conf_file):
            mock_start = create_autospec(PanoptesPluginScheduler.start, return_value=None)
            with patch('yahoo_panoptes.enrichment.enrichment_plugin_scheduler.PanoptesPluginScheduler.start',
                       mock_start):
                with self.assertRaises(SystemExit):
                    start_enrichment_plugin_scheduler()

    def test_celery_beat_service_connect_function(self):
        """
        Test if the scheduler is alive.

        Args:
            self: (todo): write your description
        """
        celery_app = self._scheduler.start()
        celery_beat_service = Service(celery_app, max_interval=None, schedule_filename=None,
                                      scheduler_cls=PanoptesCeleryPluginScheduler)

        self.assertFalse(hasattr(celery_beat_service.scheduler, 'panoptes_context'))
        self.assertFalse(hasattr(celery_beat_service.scheduler, 'metadata_kv_store_class'))
        self.assertFalse(hasattr(celery_beat_service.scheduler, 'task_prefix'))

        with patch('yahoo_panoptes.enrichment.enrichment_plugin_scheduler.enrichment_plugin_scheduler') as mock_scheduler:
            celery_beat_service_started(sender=celery_beat_service)

            self.assertTrue(hasattr(celery_beat_service.scheduler, 'panoptes_context'))
            self.assertIsNotNone(celery_beat_service.scheduler.metadata_kv_store_class)
            self.assertIsNotNone(celery_beat_service.scheduler.task_prefix)
            mock_scheduler.run.assert_called_with(celery_beat_service, None)

        with patch('yahoo_panoptes.enrichment.enrichment_plugin_scheduler.enrichment_plugin_scheduler') as mock_scheduler:
            mock_scheduler.run.side_effect = Exception
            with self.assertRaises(SystemExit):
                celery_beat_service_started(sender=celery_beat_service)
