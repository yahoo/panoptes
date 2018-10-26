"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
import logging
import os
import unittest

from mock import patch, MagicMock
from testfixtures import LogCapture

from yahoo_panoptes.framework.plugins.panoptes_base_plugin import PanoptesPluginInfo, PanoptesBasePlugin
from yahoo_panoptes.polling.polling_plugin import PanoptesPollingPlugin
from yahoo_panoptes.framework.resources import PanoptesContext, PanoptesResource
from yahoo_panoptes.framework.plugins.runner import PanoptesPluginRunner, PanoptesPluginWithEnrichmentRunner
from yahoo_panoptes.framework.metrics import PanoptesMetric, PanoptesMetricsGroupSet

from .test_framework import PanoptesTestKeyValueStore, panoptes_mock_kazoo_client, panoptes_mock_redis_strict_client


_TIMESTAMP = 1


def _callback(*args):
    pass


def _callback_no_args():
    pass


class PanoptesTestPlugin(PanoptesBasePlugin):
    name = None
    signature = None
    data = {}

    execute_now = True
    plugin_object = None

    def run(self, context):
        pass


class PanoptesTestPluginRaiseException:
    name = None
    version = None
    last_executed = None
    last_executed_age = None
    last_results = None
    last_results_age = None
    moduleMtime = None
    configMtime = None

    signature = None
    data = {}

    execute_now = True
    lock = "dummy"

    def run(self, context):
        raise Exception


class MockPluginExecuteNow:
    execute_now = False


class MockPluginLockException:
    name = None
    signature = None
    data = {}

    execute_now = True
    lock = MagicMock(side_effect=Exception)


class MockPluginLockNone:
    name = None
    signature = None
    data = {}

    execute_now = True
    lock = None


class TestPanoptesPluginRunner(unittest.TestCase):
    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = _get_test_conf_file()
        self._panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                                 key_value_store_class_list=[PanoptesTestKeyValueStore],
                                                 create_message_producer=False, async_message_producer=False,
                                                 create_zookeeper_client=True)

        self._runner_class = PanoptesPluginRunner
        self._log_capture = LogCapture()

    def tearDown(self):
        self._log_capture.uninstall()

    def test_basic_operations(self):
        runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                    None, self._panoptes_context, PanoptesTestKeyValueStore,
                                    PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                    PanoptesMetricsGroupSet, _callback)

        runner.execute_plugin()

        print '#### %s' % self._log_capture
        # Excludes log lines with 'identifier's TODO: Mock timestamps
        self._log_capture.check_present(('panoptes.tests.test_runner', 'INFO',
                                         'Attempting to execute plugin "Test Polling Plugin"'),
                                        ('panoptes.tests.test_runner', 'DEBUG',
                                         '''Starting Plugin Manager for "polling" plugins with the following '''
                                         '''configuration: {'polling': <class'''
                                         """ 'yahoo_panoptes.polling.polling_plugin.PanoptesPollingPlugin'>}, """
                                         """['tests/plugins/polling'], panoptes-plugin"""),
                                        ('panoptes.tests.test_runner', 'DEBUG', 'Found 2 plugins'),
                                        ('panoptes.tests.test_runner', 'DEBUG',
                                         'Loaded plugin '
                                         '"Test Polling Plugin", version "0.1" of type "polling"'
                                         ', category "polling"'),
                                        ('panoptes.tests.test_runner', 'DEBUG',
                                         'Loaded plugin "Test Polling Plugin Second Instance", '
                                         'version "0.1" of type "polling", category "polling"'),
                                        ('panoptes.tests.test_runner', 'INFO',
                                         '''[Test Polling Plugin:43196fb74f0346ea34a5bcaaf48c2993] [None] '''
                                         '''Attempting to get lock for plugin "Test Polling Plugin"'''),
                                        ('panoptes.tests.test_runner',
                                         'INFO',
                                         '[Test Polling Plugin:43196fb74f0346ea34a5bcaaf48c2993] [None] Acquired lock'),
                                        ('panoptes.tests.test_runner',
                                         'INFO',
                                         '[Test Polling Plugin:43196fb74f0346ea34a5bcaaf48c2993] [None] Going to run plugin "Test Polling Plugin", version "0.1", which last executed at 0 (UTC) (1540596996 seconds ago) and last produced results at 0 (UTC) (1540596996 seconds ago), module mtime 1540585191 (UTC), config mtime 1540585191 (UTC)')
                                        )

        # self._log_capture.check_present(('panoptes.tests.test_runner', 'INFO'),
        #                                 ('panoptes.tests.test_runner', 'DEBUG'),
        #                                 ('panoptes.tests.test_runner', 'DEBUG'),
        #                                 ('panoptes.tests.test_runner', 'DEBUG'),
        #                                 ('panoptes.tests.test_runner', 'DEBUG'),
        #                                 ('panoptes.tests.test_runner', 'INFO'),
        #                                 ('panoptes.tests.test_runner', 'DEBUG'),
        #                                 ('panoptes.tests.test_runner', 'INFO'),
        #                                 ('panoptes.tests.test_runner', 'INFO'),
        #                                 ('panoptes.tests.test_runner', 'INFO'),
        #                                 ('panoptes.tests.test_runner', 'INFO'),
        #                                 ('panoptes.tests.test_runner', 'INFO'),
        #                                 ('panoptes.tests.test_runner', 'INFO'),
        #                                 ('panoptes.tests.test_runner', 'INFO'),
        #                                 ('panoptes.tests.test_runner', 'INFO'),
        #                                 ('panoptes.tests.test_runner', 'INFO'),
        #                                 ('panoptes.tests.test_runner', 'INFO'),
        #                                 ('panoptes.tests.test_runner', 'INFO'),
        #                                 ('panoptes.tests.test_runner', 'DEBUG'),
        #                                 ('panoptes.tests.test_runner', 'DEBUG'))

    def test_nonexistent_plugin(self):
        runner = self._runner_class("Non-existent Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                    None, self._panoptes_context, PanoptesTestKeyValueStore,
                                    PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                    PanoptesMetricsGroupSet, _callback)
        runner.execute_plugin()
        self._log_capture.check_present(('panoptes.tests.test_runner', 'INFO',
                                         'Attempting to execute plugin "Non-existent Plugin"'),
                                        ('panoptes.tests.test_runner', 'DEBUG',
                                         'Starting Plugin Manager for "polling" plugins with the following '
                                         "configuration: {'polling': <class 'yahoo_panoptes.polling.polling_plugin."
                                         "PanoptesPollingPlugin'>}, "
                                         "['tests/plugins/polling'], panoptes-plugin"),
                                        ('panoptes.tests.test_runner', 'DEBUG', 'Found 2 plugins'),
                                        ('panoptes.tests.test_runner', 'DEBUG',
                                         'Loaded plugin "Test Polling Plugin", version "0.1" of type "polling", '
                                         'category "polling"'),
                                        ('panoptes.tests.test_runner', 'DEBUG',
                                         'Loaded plugin "Test Polling Plugin Second Instance", version "0.1" of type '
                                         '"polling", category "polling"'),
                                        ('panoptes.tests.test_runner', 'WARNING',
                                         'No plugin named "Non-existent Plugin" found in "'
                                         '''['tests/plugins/polling']"'''))

    def test_bad_plugin_type(self):
        runner = self._runner_class("Test Polling Plugin", "bad", PanoptesPollingPlugin, PanoptesPluginInfo,
                                    None, self._panoptes_context, PanoptesTestKeyValueStore,
                                    PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                    PanoptesMetricsGroupSet, _callback)
        runner.execute_plugin()

        self._log_capture.check_present(('panoptes.tests.test_runner', 'ERROR',
                                         '''Error trying to load plugin "Test Polling Plugin": KeyError('bad',)'''))

    def test_execute_now_false(self):
        mock_get_plugin_by_name = MagicMock(return_value=MockPluginExecuteNow())
        with patch('yahoo_panoptes.framework.plugins.runner.PanoptesPluginManager.getPluginByName',
                   mock_get_plugin_by_name):
            runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                        None, self._panoptes_context, PanoptesTestKeyValueStore,
                                        PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                        PanoptesMetricsGroupSet, _callback)
            runner.execute_plugin()

            self._log_capture.check_present(('panoptes.tests.test_runner', 'INFO',
                                             'Attempting to execute plugin "Test Polling Plugin"'),
                                            ('panoptes.tests.test_runner', 'DEBUG', '''Starting Plugin Manager for ''' 
                                            '''"polling" plugins with the following configuration: {'polling': '''
                                            """<class 'yahoo_panoptes.polling.polling_plugin.PanoptesPollingPlugin'"""
                                            """>}, ['tests/plugins/polling'], panoptes-plugin"""),
                                            ('panoptes.tests.test_runner', 'DEBUG', 'Found 2 plugins'),
                                            ('panoptes.tests.test_runner', 'DEBUG',
                                             'Loaded plugin '
                                             '"Test Polling Plugin", version "0.1" of type "polling"'
                                             ', category "polling"'),
                                            ('panoptes.tests.test_runner', 'DEBUG',
                                             'Loaded plugin "Test Polling Plugin Second Instance", '
                                             'version "0.1" of type "polling", category "polling"'))

    def test_callback_failure(self):
        runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                    None, self._panoptes_context, PanoptesTestKeyValueStore,
                                    PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                    PanoptesMetricsGroupSet, _callback_no_args)
        runner.execute_plugin()

        self._log_capture.check_present(('panoptes.tests.test_runner', 'ERROR',
                                         '[Test Polling Plugin:43196fb74f0346ea34a5bcaaf48c2993] '
                                         '[None] Results callback function failed:'))

    def test_lock(self):
        mock_get_plugin_by_name = MagicMock(return_value=MockPluginLockNone())
        mock_get_context = MagicMock(return_value=self._panoptes_context)
        with patch('yahoo_panoptes.framework.plugins.runner.PanoptesPluginManager.getPluginByName',
                   mock_get_plugin_by_name):
            with patch('yahoo_panoptes.framework.plugins.runner.PanoptesPluginRunner._get_context', mock_get_context):
                runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin,
                                            PanoptesPluginInfo, None, self._panoptes_context, PanoptesTestKeyValueStore,
                                            PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                            PanoptesMetricsGroupSet, _callback)
                runner.execute_plugin()

                self._log_capture.check_present(('panoptes.tests.test_runner', 'INFO',
                                                 '[None:None] [{}] Attempting to get lock for plugin'
                                                 ' "Test Polling Plugin"'))

    def test_lock_error(self):
        mock_plugin = MagicMock(return_value=PanoptesTestPlugin)
        mock_get_context = MagicMock(return_value=self._panoptes_context)
        with patch('yahoo_panoptes.framework.plugins.runner.PanoptesPluginManager.getPluginByName',
                   mock_plugin):
            with patch('yahoo_panoptes.framework.plugins.runner.PanoptesPluginRunner._get_context', mock_get_context):
                runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                            None, self._panoptes_context, PanoptesTestKeyValueStore,
                                            PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                            PanoptesMetricsGroupSet, _callback)
                runner.execute_plugin()

                self._log_capture.check_present(('panoptes.tests.test_runner', 'ERROR',
                                                 '[None:None] [{}] Error in acquiring lock:'))

    def test_plugin_failure(self):
        mock_plugin = MagicMock(return_value=PanoptesTestPluginRaiseException)
        mock_get_context = MagicMock(return_value=self._panoptes_context)
        with patch('yahoo_panoptes.framework.plugins.runner.PanoptesPluginManager.getPluginByName',
                   mock_plugin):
            with patch('yahoo_panoptes.framework.plugins.runner.PanoptesPluginRunner._get_context', mock_get_context):
                runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                            None, self._panoptes_context, PanoptesTestKeyValueStore,
                                            PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                            PanoptesMetricsGroupSet, _callback)
                runner.execute_plugin()

                self._log_capture.check_present(('panoptes.tests.test_runner', 'ERROR',
                                                 '[None:None] [{}] Failed to execute plugin:'),
                                                ('panoptes.tests.test_runner', 'INFO',
                                                 '[None:None] [{}] Ran in 0.00 seconds'),
                                                ('panoptes.tests.test_runner', 'ERROR',
                                                 '[None:None] [{}] Failed to release lock for plugin:'),
                                                ('panoptes.tests.test_runner', 'WARNING',
                                                 '[None:None] [{}] Plugin did not return any results'))

    def test_plugin_wrong_result_type(self):
        runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                    None, self._panoptes_context, PanoptesTestKeyValueStore,
                                    PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                    PanoptesMetric, _callback)
        runner.execute_plugin()

    def test_logging_methods(self):
        runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                    None, self._panoptes_context, PanoptesTestKeyValueStore,
                                    PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                    PanoptesMetricsGroupSet, _callback)

        #  Ensure logging methods run:
        runner.info(PanoptesTestPlugin(), "Test Info log message")
        runner.warn(PanoptesTestPlugin(), "Test Warning log message")
        runner.error(PanoptesTestPlugin(), "Test Error log message", Exception)
        runner.exception(PanoptesTestPlugin(), "Test Exception log message")


class TestPanoptesPluginWithEnrichmentRunner(TestPanoptesPluginRunner):
    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = _get_test_conf_file()
        self._panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                                 key_value_store_class_list=[PanoptesTestKeyValueStore],
                                                 create_message_producer=False, async_message_producer=False,
                                                 create_zookeeper_client=True)
        self._panoptes_resource = PanoptesResource(resource_site="test", resource_class="test",
                                                   resource_subclass="test", resource_type="test", resource_id="test",
                                                   resource_endpoint="test", resource_creation_timestamp=_TIMESTAMP,
                                                   resource_plugin="test")
        self._runner_class = PanoptesPluginWithEnrichmentRunner

    def test_basic_operations(self):
        super(TestPanoptesPluginWithEnrichmentRunner, self).test_basic_operations()

        # Test where enrichment is None
        mock_panoptes_enrichment_cache = MagicMock(return_value=None)
        with patch('yahoo_panoptes.framework.plugins.runner.PanoptesEnrichmentCache', mock_panoptes_enrichment_cache):
            runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                        self._panoptes_resource, self._panoptes_context, PanoptesTestKeyValueStore,
                                        PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                        PanoptesMetricsGroupSet, _callback)
            runner.execute_plugin()

        # Test with enrichment
        runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                    self._panoptes_resource, self._panoptes_context, PanoptesTestKeyValueStore,
                                    PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                    PanoptesMetricsGroupSet, _callback)
        runner.execute_plugin()

    def test_lock(self):
        mock_get_plugin_by_name = MagicMock(return_value=MockPluginLockNone())
        mock_get_context = MagicMock(return_value=self._panoptes_context)
        with patch('yahoo_panoptes.framework.plugins.runner.PanoptesPluginManager.getPluginByName',
                   mock_get_plugin_by_name):
            with patch('yahoo_panoptes.framework.plugins.runner.PanoptesPluginWithEnrichmentRunner._get_context',
                       mock_get_context):
                runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                            None, self._panoptes_context, PanoptesTestKeyValueStore,
                                            PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                            PanoptesMetricsGroupSet, _callback)
                runner.execute_plugin()

    def test_lock_error(self):
        mock_plugin = MagicMock(return_value=PanoptesTestPlugin)
        mock_get_context = MagicMock(return_value=self._panoptes_context)
        with patch('yahoo_panoptes.framework.plugins.runner.PanoptesPluginManager.getPluginByName',
                   mock_plugin):
            with patch('yahoo_panoptes.framework.plugins.runner.PanoptesPluginWithEnrichmentRunner._get_context',
                       mock_get_context):
                runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                            None, self._panoptes_context, PanoptesTestKeyValueStore,
                                            PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                            PanoptesMetricsGroupSet, _callback)
                runner.execute_plugin()

    def test_plugin_failure(self):
        mock_plugin = MagicMock(return_value=PanoptesTestPluginRaiseException)
        mock_get_context = MagicMock(return_value=self._panoptes_context)
        with patch('yahoo_panoptes.framework.plugins.runner.PanoptesPluginManager.getPluginByName',
                   mock_plugin):
            with patch('yahoo_panoptes.framework.plugins.runner.PanoptesPluginWithEnrichmentRunner._get_context',
                       mock_get_context):
                runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                            None, self._panoptes_context, PanoptesTestKeyValueStore,
                                            PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                            PanoptesMetricsGroupSet, _callback)
                runner.execute_plugin()

    def test_plugin_wrong_result_type(self):
        runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                    None, self._panoptes_context, PanoptesTestKeyValueStore,
                                    PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                    PanoptesMetric, _callback)
        runner.execute_plugin()

    def test_logging_methods(self):
        runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                    None, self._panoptes_context, PanoptesTestKeyValueStore,
                                    PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                    PanoptesMetricsGroupSet, _callback)

        #  Ensure logging methods run:
        runner.info(PanoptesTestPlugin(), "Test Info log message")
        runner.warn(PanoptesTestPlugin(), "Test Warning log message")
        runner.error(PanoptesTestPlugin(), "Test Error log message", Exception)
        runner.exception(PanoptesTestPlugin(), "Test Exception log message")


def _get_test_conf_file():
    my_dir = os.path.dirname(os.path.realpath(__file__))
    panoptes_test_conf_file = os.path.join(my_dir, 'config_files/test_panoptes_config.ini')

    return my_dir, panoptes_test_conf_file
