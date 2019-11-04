"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
from __future__ import unicode_literals
from __future__ import absolute_import
from builtins import object
import re
import unittest
import json

from mock import patch, MagicMock, Mock, PropertyMock
from testfixtures import LogCapture

from yahoo_panoptes.framework.utilities.helpers import ordered
from yahoo_panoptes.framework.plugins.panoptes_base_plugin import PanoptesPluginInfo, PanoptesBasePlugin
from yahoo_panoptes.polling.polling_plugin import PanoptesPollingPlugin
from yahoo_panoptes.polling.polling_plugin_agent import polling_plugin_task, PanoptesPollingPluginKeyValueStore, \
    PanoptesSecretsStore, PanoptesPollingPluginAgentKeyValueStore

from yahoo_panoptes.discovery.discovery_plugin_agent import PanoptesDiscoveryPluginAgentKeyValueStore, \
    PanoptesDiscoveryPluginKeyValueStore, PanoptesSecretsStore, discovery_plugin_task

from yahoo_panoptes.framework.resources import PanoptesContext, PanoptesResource, PanoptesResourcesKeyValueStore
from yahoo_panoptes.framework.plugins.runner import PanoptesPluginRunner, PanoptesPluginWithEnrichmentRunner
from yahoo_panoptes.framework.metrics import PanoptesMetric, PanoptesMetricsGroupSet


from tests.mock_panoptes_producer import MockPanoptesMessageProducer

from .test_framework import PanoptesTestKeyValueStore, panoptes_mock_kazoo_client, panoptes_mock_redis_strict_client
from .helpers import get_test_conf_file

_TIMESTAMP = 1


def _callback(*args):
    pass


def _callback_with_exception(*args):
    raise Exception


class PanoptesTestPluginNoLock(PanoptesBasePlugin):
    name = None
    signature = None
    data = {}

    execute_now = True
    plugin_object = None

    def run(self, context):
        pass


class PanoptesTestPluginRaisePluginReleaseException(object):
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
    lock = MagicMock(locked=True, release=MagicMock(side_effect=Exception))

    def run(self, context):
        raise Exception


class MockPluginExecuteNow(object):
    execute_now = False


class MockPluginLockException(object):
    name = None
    signature = None
    data = {}

    execute_now = True
    lock = MagicMock(side_effect=Exception)


class MockPluginLockNone(object):
    name = None
    signature = None
    data = {}

    execute_now = True
    lock = None


class MockPluginLockIsNotLocked(object):
    name = None
    signature = None
    data = {}

    execute_now = True
    lock = MagicMock(locked=False)


_, global_panoptes_test_conf_file = get_test_conf_file()


class TestPanoptesPluginRunner(unittest.TestCase):

    @staticmethod
    def extract(record):
        message = record.getMessage()

        match_obj = re.match(r'(?P<name>.*):\w+(?P<body>.*)', message)
        if match_obj:
            message = match_obj.group('name') + match_obj.group('body')

        match_obj = re.match(r'(?P<start>.*[R|r]an in\s)\d+\.?\d*.*(?P<end>seconds.*)', message)
        if match_obj:
            return record.name, record.levelname, match_obj.group('start') + match_obj.group('end')

        match_obj = re.match(r'(?P<start>.*took\s*)\d+\.?\d*.*(?P<seconds>seconds\D*)\d+\s(?P<end>garbage objects.*)',
                             message)

        if match_obj:
            return record.name, record.levelname, match_obj.group('start') + match_obj.group('seconds') + \
                   match_obj.group('end')

        match_obj = re.match(
            r'(?P<start>Attempting to get lock for plugin .*with lock path) \".*\".*(?P<id> and identifier).*'
            r'(?P<in> in) \d\.?\d*(?P<seconds> seconds)',
            message)
        if match_obj:
            return record.name, record.levelname, match_obj.group('start') + match_obj.group('id') + \
                   match_obj.group('in') + match_obj.group('seconds')

        match_obj = re.match(
            r'(?P<delete>Deleting module: yapsy_loaded_plugin_Test_Polling_Plugin_Second_Instance|'
            r'Deleting module: yapsy_loaded_plugin_Test_Polling_Plugin).*',
            message
        )

        if match_obj:
            return record.name, record.levelname, match_obj.group('delete')

        return record.name, record.levelname, message

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = get_test_conf_file()
        self._panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                                 key_value_store_class_list=[PanoptesTestKeyValueStore,
                                                                             PanoptesResourcesKeyValueStore,
                                                                             PanoptesPollingPluginKeyValueStore,
                                                                             PanoptesSecretsStore,
                                                                             PanoptesPollingPluginAgentKeyValueStore,
                                                                             PanoptesDiscoveryPluginAgentKeyValueStore,
                                                                             PanoptesDiscoveryPluginKeyValueStore],
                                                 create_message_producer=False, async_message_producer=False,
                                                 create_zookeeper_client=True)

        self._runner_class = PanoptesPluginRunner
        self._log_capture = LogCapture(attributes=self.extract)

    def tearDown(self):
        self._log_capture.uninstall()

    def test_logging_methods(self):
        runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                    None, self._panoptes_context, PanoptesTestKeyValueStore,
                                    PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                    PanoptesMetricsGroupSet, _callback)

        #  Ensure logging methods run:
        runner.info(PanoptesTestPluginNoLock(), "Test Info log message")
        runner.warn(PanoptesTestPluginNoLock(), "Test Warning log message")
        runner.error(PanoptesTestPluginNoLock(), "Test Error log message", Exception)
        runner.exception(PanoptesTestPluginNoLock(), "Test Exception log message")

        self._log_capture.check_present(('panoptes.tests.test_runner', 'INFO', '[None] [{}] Test Info log message'),
                                        ('panoptes.tests.test_runner', 'WARNING',
                                         '[None] [{}] Test Warning log message'),
                                        ('panoptes.tests.test_runner', 'ERROR',
                                         '[None] [{}] Test Exception log message:'),
                                        order_matters=False)

    def test_basic_operations(self):
        runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                    None, self._panoptes_context, PanoptesTestKeyValueStore,
                                    PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                    PanoptesMetricsGroupSet, _callback)

        runner.execute_plugin()

        self._log_capture.check_present(('panoptes.tests.test_runner', 'INFO',
                                         'Attempting to execute plugin "Test Polling Plugin"'),
                                        ('panoptes.tests.test_runner', 'DEBUG', 'Found 3 plugins'),
                                        ('panoptes.tests.test_runner', 'DEBUG',
                                         'Loaded plugin '
                                         '"Test Polling Plugin", version "0.1" of type "polling"'
                                         ', category "polling"'),
                                        ('panoptes.tests.test_runner',
                                         'DEBUG',
                                         'Loaded plugin "Test Polling Plugin 2", '
                                         'version "0.1" of type "polling", category "polling"'),
                                        ('panoptes.tests.test_runner', 'DEBUG',
                                         'Loaded plugin "Test Polling Plugin Second Instance", '
                                         'version "0.1" of type "polling", category "polling"'),
                                        ('panoptes.tests.test_runner', 'INFO',
                                         '''[Test Polling Plugin] [None] '''
                                         '''Attempting to get lock for plugin "Test Polling Plugin"'''),
                                        ('panoptes.tests.test_runner', 'DEBUG',
                                         'Attempting to get lock for plugin "Test Polling Plugin", with lock path and '
                                         'identifier in seconds'),
                                        ('panoptes.tests.test_runner',
                                         'INFO',
                                         '[Test Polling Plugin] [None] Acquired lock'),
                                        ('panoptes.tests.test_runner',
                                         'INFO',
                                         '[Test Polling Plugin] [None]'
                                         ' Ran in seconds'),
                                        ('panoptes.tests.test_runner',
                                         'INFO',
                                         '[Test Polling Plugin] [None] Released lock'),
                                        ('panoptes.tests.test_runner',
                                         'INFO',
                                         '[Test Polling Plugin] [None] Plugin returned'
                                         ' a result set with 1 members'),
                                        ('panoptes.tests.test_runner',
                                         'INFO',
                                         '[Test Polling Plugin] [None]'
                                         ' Callback function ran in seconds'),
                                        ('panoptes.tests.test_runner',
                                         'INFO',
                                         '[Test Polling Plugin] [None] GC took seconds. There are garbage objects.'),
                                        ('panoptes.tests.test_runner',
                                         'DEBUG',
                                         'Deleting module: yapsy_loaded_plugin_Test_Polling_Plugin'),
                                        ('panoptes.tests.test_runner',
                                         'DEBUG',
                                         'Deleting module: yapsy_loaded_plugin_Test_Polling_Plugin'),
                                        ('panoptes.tests.test_runner',
                                         'DEBUG',
                                         'Deleting module: yapsy_loaded_plugin_Test_Polling_Plugin_Second_Instance'),
                                        order_matters=False
                                        )

    def test_nonexistent_plugin(self):
        runner = self._runner_class("Non-existent Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                    None, self._panoptes_context, PanoptesTestKeyValueStore,
                                    PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                    PanoptesMetricsGroupSet, _callback)
        runner.execute_plugin()
        self._log_capture.check_present(('panoptes.tests.test_runner', 'INFO',
                                         'Attempting to execute plugin "Non-existent Plugin"'),
                                        ('panoptes.tests.test_runner', 'DEBUG', 'Found 3 plugins'),
                                        ('panoptes.tests.test_runner', 'DEBUG',
                                         'Loaded plugin "Test Polling Plugin", version "0.1" of type "polling", '
                                         'category "polling"'),
                                        ('panoptes.tests.test_runner', 'DEBUG',
                                         'Loaded plugin "Test Polling Plugin Second Instance", version "0.1" of type '
                                         '"polling", category "polling"'),
                                        ('panoptes.tests.test_runner', 'WARNING',
                                         'No plugin named "Non-existent Plugin" found in "'
                                         '''['tests/plugins/polling']"'''),
                                        order_matters=False)

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
                                            ('panoptes.tests.test_runner', 'DEBUG', 'Found 3 plugins'),
                                            ('panoptes.tests.test_runner', 'DEBUG',
                                             'Loaded plugin '
                                             '"Test Polling Plugin", version "0.1" of type "polling"'
                                             ', category "polling"'),
                                            ('panoptes.tests.test_runner', 'DEBUG',
                                             'Loaded plugin "Test Polling Plugin Second Instance", '
                                             'version "0.1" of type "polling", category "polling"'),
                                            order_matters=False)

    def test_callback_failure(self):
        runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                    None, self._panoptes_context, PanoptesTestKeyValueStore,
                                    PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                    PanoptesMetricsGroupSet, _callback_with_exception)
        runner.execute_plugin()

        self._log_capture.check_present(('panoptes.tests.test_runner', 'ERROR',
                                         '[Test Polling Plugin] '
                                         '[None] Results callback function failed: :'))

    def test_lock_no_lock_object(self):
        mock_plugin = MagicMock(return_value=PanoptesTestPluginNoLock)
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
                                                 '[None] [{}] Error in acquiring lock:'))

    def test_lock_is_none(self):
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
                                                 '[None] [{}] Attempting to get lock for plugin'
                                                 ' "Test Polling Plugin"'))

    def test_lock_is_not_locked(self):
        mock_get_plugin_by_name = MagicMock(return_value=MockPluginLockIsNotLocked())
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
                                                 '[None] [{}] Attempting to get lock for plugin'
                                                 ' "Test Polling Plugin"'))

    def test_plugin_failure(self):
        mock_plugin = MagicMock(return_value=PanoptesTestPluginRaisePluginReleaseException)
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
                                                 '[None] [{}] Failed to execute plugin:'),
                                                ('panoptes.tests.test_runner', 'INFO',
                                                 '[None] [{}] Ran in seconds'),
                                                ('panoptes.tests.test_runner', 'ERROR',
                                                 '[None] [{}] Failed to release lock for plugin:'),
                                                ('panoptes.tests.test_runner', 'WARNING',
                                                 '[None] [{}] Plugin did not return any results'),
                                                order_matters=False)

    def test_plugin_wrong_result_type(self):
        runner = self._runner_class("Test Polling Plugin 2", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                    None, self._panoptes_context, PanoptesTestKeyValueStore,
                                    PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                    PanoptesMetricsGroupSet, _callback)
        runner.execute_plugin()

        self._log_capture.check_present(('panoptes.tests.test_runner', 'WARNING',
                                         '[Test Polling Plugin 2] [None] Plugin returned an unexpected result type: '
                                         '"PanoptesMetricsGroup"'))


class TestPanoptesPluginWithEnrichmentRunner(TestPanoptesPluginRunner):
    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def setUp(self):

        self.my_dir, self.panoptes_test_conf_file = get_test_conf_file()
        self._panoptes_resource = PanoptesResource(resource_site="test", resource_class="test",
                                                   resource_subclass="test", resource_type="test", resource_id="test",
                                                   resource_endpoint="test", resource_creation_timestamp=_TIMESTAMP,
                                                   resource_plugin="test")

        self._panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                                 key_value_store_class_list=[PanoptesTestKeyValueStore,
                                                                             PanoptesResourcesKeyValueStore,
                                                                             PanoptesPollingPluginKeyValueStore,
                                                                             PanoptesSecretsStore,
                                                                             PanoptesPollingPluginAgentKeyValueStore,
                                                                             PanoptesDiscoveryPluginAgentKeyValueStore,
                                                                             PanoptesDiscoveryPluginKeyValueStore],
                                                 create_message_producer=False, async_message_producer=False,
                                                 create_zookeeper_client=True)
        self._runner_class = PanoptesPluginWithEnrichmentRunner

        self._log_capture = LogCapture(attributes=TestPanoptesPluginRunner.extract)

    def tearDown(self):
        self._log_capture.uninstall()

    def test_basic_operations(self):
        # Test where enrichment is None
        mock_panoptes_enrichment_cache = Mock(return_value=None)
        with patch('yahoo_panoptes.framework.plugins.runner.PanoptesEnrichmentCache', mock_panoptes_enrichment_cache):
            runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                        self._panoptes_resource, self._panoptes_context, PanoptesTestKeyValueStore,
                                        PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                        PanoptesMetricsGroupSet, _callback)
            runner.execute_plugin()

            self._log_capture.check_present(('panoptes.tests.test_runner', 'ERROR',
                                             '[Test Polling Plugin] [plugin|test|site|test|class|test|subclass|test|'
                                             'type|test|id|test|endpoint|test] '
                                             'Could not setup context for plugin:'),
                                            order_matters=False
                                            )
            self._log_capture.uninstall()

        self._log_capture = LogCapture(attributes=TestPanoptesPluginRunner.extract)
        # Test with enrichment
        runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                    self._panoptes_resource, self._panoptes_context, PanoptesTestKeyValueStore,
                                    PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                    PanoptesMetricsGroupSet, _callback)
        runner.execute_plugin()

        self._log_capture.check_present(('panoptes.tests.test_runner', 'INFO',
                                         'Attempting to execute plugin "Test Polling Plugin"'),
                                        ('panoptes.tests.test_runner', 'DEBUG', 'Found 3 plugins'),
                                        ('panoptes.tests.test_runner', 'DEBUG',
                                         'Loaded plugin '
                                         '"Test Polling Plugin", version "0.1" of type "polling"'
                                         ', category "polling"'),
                                        ('panoptes.tests.test_runner',
                                         'DEBUG',
                                         'Loaded plugin "Test Polling Plugin 2", '
                                         'version "0.1" of type "polling", category "polling"'),
                                        ('panoptes.tests.test_runner', 'DEBUG',
                                         'Loaded plugin "Test Polling Plugin Second Instance", '
                                         'version "0.1" of type "polling", category "polling"'),
                                        ('panoptes.tests.test_runner',
                                         'INFO',
                                         '[Test Polling Plugin] [plugin|test|site|test|class|test|subclass|test|'
                                         'type|test|id|test|endpoint|test] Attempting to get lock for plugin '
                                         '"Test Polling Plugin"'),
                                        ('panoptes.tests.test_runner', 'DEBUG',
                                         'Attempting to get lock for plugin "Test Polling Plugin", with lock path and '
                                         'identifier in seconds'),
                                        ('panoptes.tests.test_runner',
                                         'INFO',
                                         '[Test Polling Plugin] [plugin|test|site|test|class|test|subclass|test|'
                                         'type|test|id|test|endpoint|test] Acquired lock'),
                                        ('panoptes.tests.test_runner',
                                         'INFO',
                                         '[Test Polling Plugin] [plugin|test|site|test|class|test|subclass|test|'
                                         'type|test|id|test|endpoint|test]'
                                         ' Ran in seconds'),
                                        ('panoptes.tests.test_runner',
                                         'INFO',
                                         '[Test Polling Plugin] [plugin|test|site|test|class|test|subclass|test|'
                                         'type|test|id|test|endpoint|test] Released lock'),
                                        ('panoptes.tests.test_runner',
                                         'INFO',
                                         '[Test Polling Plugin] [plugin|test|site|test|class|test|subclass|test|'
                                         'type|test|id|test|endpoint|test] Plugin returned'
                                         ' a result set with 1 members'),
                                        ('panoptes.tests.test_runner',
                                         'INFO',
                                         '[Test Polling Plugin] [plugin|test|site|test|class|test|subclass|test|'
                                         'type|test|id|test|endpoint|test]'
                                         ' Callback function ran in seconds'),
                                        ('panoptes.tests.test_runner',
                                         'INFO',
                                         '[Test Polling Plugin] [plugin|test|site|test|class|test|subclass|test|type|'
                                         'test|id|test|endpoint|test] GC took seconds. There are garbage objects.'),
                                        ('panoptes.tests.test_runner',
                                         'ERROR',
                                         'No enrichment data found on KV store for plugin Test Polling Plugin '
                                         'resource test namespace test using key test'),
                                        ('panoptes.tests.test_runner',
                                         'DEBUG',
                                         'Successfully created PanoptesEnrichmentCache enrichment_data {} for plugin '
                                         'Test Polling Plugin'),
                                        order_matters=False
                                        )

    def test_callback_failure(self):
        runner = self._runner_class("Test Polling Plugin", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                    self._panoptes_resource, self._panoptes_context, PanoptesTestKeyValueStore,
                                    PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                    PanoptesMetricsGroupSet, _callback_with_exception)
        runner.execute_plugin()

        self._log_capture.check_present(('panoptes.tests.test_runner', 'ERROR',
                                         '[Test Polling Plugin] '
                                         '[plugin|test|site|test|class|test|subclass|test|'
                                         'type|test|id|test|endpoint|test] Results callback function failed: :'))

    # 'pass' is needed for these methods because the only difference in their logging output from
    # TestPanoptesPluginRunner is the presence of the PanoptesResource in some log messages.
    def test_lock_no_lock_object(self):
        pass

    def test_lock_is_none(self):
        pass

    def test_lock_is_not_locked(self):
        pass

    def test_plugin_failure(self):
        pass

    def test_plugin_wrong_result_type(self):
        runner = self._runner_class("Test Polling Plugin 2", "polling", PanoptesPollingPlugin, PanoptesPluginInfo,
                                    None, self._panoptes_context, PanoptesTestKeyValueStore,
                                    PanoptesTestKeyValueStore, PanoptesTestKeyValueStore, "plugin_logger",
                                    PanoptesMetric, _callback)
        runner.execute_plugin()

        self._log_capture.check_present(('panoptes.tests.test_runner',
                                         'ERROR',
                                         '[Test Polling Plugin 2] [None] Could not setup context for plugin:'))


class TestPanoptesPollingPluginRunner(unittest.TestCase):

    @patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def setUp(self):

        self.my_dir, self.panoptes_test_conf_file = get_test_conf_file()
        self._panoptes_resource = PanoptesResource(resource_site="test", resource_class="test",
                                                   resource_subclass="test", resource_type="test", resource_id="test",
                                                   resource_endpoint="test", resource_creation_timestamp=_TIMESTAMP,
                                                   resource_plugin="test")

        self._panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                                 key_value_store_class_list=[PanoptesTestKeyValueStore,
                                                                             PanoptesResourcesKeyValueStore,
                                                                             PanoptesPollingPluginKeyValueStore,
                                                                             PanoptesSecretsStore,
                                                                             PanoptesPollingPluginAgentKeyValueStore,
                                                                             PanoptesDiscoveryPluginAgentKeyValueStore,
                                                                             PanoptesDiscoveryPluginKeyValueStore],
                                                 create_message_producer=False, async_message_producer=False,
                                                 create_zookeeper_client=True)
        self._runner_class = PanoptesPluginWithEnrichmentRunner

        self._log_capture = LogCapture(attributes=TestPanoptesPluginRunner.extract)

    def tearDown(self):
        self._log_capture.uninstall()

    def tearDown(self):
        self._log_capture.uninstall()

    @patch('yahoo_panoptes.framework.metrics.time')
    @patch('yahoo_panoptes.framework.context.PanoptesContext._get_message_producer')
    @patch('yahoo_panoptes.framework.context.PanoptesContext.message_producer', new_callable=PropertyMock)
    @patch('yahoo_panoptes.polling.polling_plugin_agent.PanoptesPollingTaskContext')
    @patch('yahoo_panoptes.framework.resources.PanoptesResourceStore.get_resource')
    def test_polling_plugin_agent(self, resource, panoptes_context, message_producer, message_producer_property, time):

        producer = MockPanoptesMessageProducer()
        time.return_value = 1
        message_producer.return_value = producer
        message_producer_property.return_value = producer
        resource.return_value = self._panoptes_resource
        panoptes_context.return_value = self._panoptes_context

        polling_plugin_task('Test Polling Plugin', 'polling')

        log_prefix = '[Test Polling Plugin] [plugin|test|site|test|class|test|' \
                     'subclass|test|type|test|id|test|endpoint|test]'

        self._log_capture.check_present(
            ('panoptes.tests.test_runner', 'INFO', 'Attempting to execute plugin "Test Polling Plugin"'),
            ('panoptes.tests.test_runner', 'DEBUG', 'Loaded plugin "Test Polling Plugin", '
                                                    'version "0.1" of type "polling", category "polling"'),
            ('panoptes.tests.test_runner', 'DEBUG', 'Loaded plugin "Test Polling Plugin 2", '
                                                    'version "0.1" of type "polling", category "polling"'),
            ('panoptes.tests.test_runner', 'ERROR', 'No enrichment data found on KV store for plugin Test'
                                                    ' Polling Plugin resource test namespace test using key test'),
            ('panoptes.tests.test_runner', 'DEBUG', 'Successfully created PanoptesEnrichmentCache enrichment_data '
                                                    '{} for plugin Test Polling Plugin'),
            ('panoptes.tests.test_runner', 'DEBUG', 'Attempting to get lock for plugin "Test Polling Plugin", '
                                                    'with lock path and identifier in seconds'),
            ('panoptes.tests.test_runner', 'INFO', '{} Acquired lock'.format(log_prefix)),
            ('panoptes.tests.test_runner', 'INFO', '{} Plugin returned a result set with 1 members'.format(log_prefix)),
            ('panoptes.tests.test_runner', 'INFO', '{} Callback function ran in seconds'.format(log_prefix)),
            ('panoptes.tests.test_runner', 'INFO', '{} Ran in seconds'.format(log_prefix)),
            ('panoptes.tests.test_runner', 'INFO', '{} Released lock'.format(log_prefix)),
            ('panoptes.tests.test_runner', 'INFO', '{} GC took seconds. There are garbage objects.'.format(log_prefix)),
            ('panoptes.tests.test_runner', 'DEBUG', 'Deleting module: yapsy_loaded_plugin_Test_Polling_Plugin'),
            ('panoptes.tests.test_runner', 'DEBUG', 'Deleting module: yapsy_loaded_plugin_Test_Polling_Plugin'),
            ('panoptes.tests.test_runner', 'DEBUG', 'Deleting module: '
                                                    'yapsy_loaded_plugin_Test_Polling_Plugin_Second_Instance'),
            order_matters=False
        )

        kafka_push_log = {
            "metrics_group_type": "Test",
            "metrics_group_interval": 60,
            "metrics_group_creation_timestamp": 1,
            "metrics_group_schema_version": "0.2",
            "resource": {
                "resource_site": "test",
                "resource_class": "test",
                "resource_subclass": "test",
                "resource_type": "test",
                "resource_id": "test",
                "resource_endpoint": "test",
                "resource_metadata": {
                    "_resource_ttl": "604800"
                },
                "resource_creation_timestamp": 1.0,
                "resource_plugin": "test"
            }, "metrics": [
                {
                    "metric_creation_timestamp": 1,
                    "metric_name": "test",
                    "metric_value": 0.0,
                    "metric_type": "gauge"
                }
            ],
            "dimensions": []
        }

        # Timestamps need to be removed to check Panoptes Metrics
        metric_groups_seen = 0
        for line in self._log_capture.actual():

            _, _, log = line

            if 'resource_creation_timestamp' in log:
                log = re.sub(r"resource_creation_timestamp\": \d+\.\d+,",
                             "resource_creation_timestamp\": 1.0,",
                             log)
                resource_match = re.search(r'{.*}', log)

                if resource_match is not None:
                    self.assertEqual(ordered(json.loads(resource_match.group(0))), ordered(kafka_push_log))

            if log.startswith('Sent metric group'):
                metric_groups_seen += 1

            if log.startswith('Going to send metric group'):
                metric_groups_seen += 1

        self.assertEqual(metric_groups_seen, 2)


class TestPanoptesDiscoveryPluginRunner(TestPanoptesPluginRunner):

    @patch('yahoo_panoptes.framework.context.PanoptesContext._get_message_producer')
    @patch('yahoo_panoptes.framework.context.PanoptesContext.message_producer', new_callable=PropertyMock)
    @patch('yahoo_panoptes.discovery.discovery_plugin_agent.PanoptesDiscoveryTaskContext')
    def test_discovery_plugin_task(self, panoptes_context, message_producer_property, message_producer):

        producer = MockPanoptesMessageProducer()
        message_producer_property.return_value = message_producer.return_value = producer
        panoptes_context.return_value = self._panoptes_context

        discovery_plugin_task("Test Discovery Plugin")

        plugin_result = producer.messages
        self.assertEqual(len(plugin_result), 1)

        plugin_result = plugin_result[0]
        self.assertTrue('Test_Discovery_Plugin' in plugin_result['key'])
        plugin_result['key'] = 'Test_Discovery_Plugin'

        expected_result = {
            'topic': 'test_site-resources',
            'message': '{"resource_set_creation_timestamp": 1.0, '
                       '"resource_set_schema_version": "0.1", "resources": '
                       '[{"resource_site": "test_site", "resource_class": '
                       '"test_class", "resource_subclass": "test_subclass", '
                       '"resource_type": "test_type", "resource_id": '
                       '"test_resource_id", "resource_endpoint": '
                       '"test_resource_endpoint", "resource_metadata": '
                       '{"_resource_ttl": "604800"},'
                       ' "resource_creation_timestamp": 1.0,'
                       ' "resource_plugin": "test_resource_plugin"}]}',
            'key': 'Test_Discovery_Plugin'}

        plugin_result['message'] = re.sub(
            r"resource_set_creation_timestamp\": \d+\.\d+,",
            "resource_set_creation_timestamp\": 1.0,",
            plugin_result['message'])

        plugin_result['message'] = re.sub(
            r"resource_creation_timestamp\": \d+\.\d+,",
            "resource_creation_timestamp\": 1.0,",
            plugin_result['message'])

        self.assertEqual(plugin_result['topic'], expected_result['topic'])
        self.assertEqual(plugin_result['key'], expected_result['key'])

        self.assertEqual(
            ordered(json.loads(plugin_result['message'])),
            ordered(json.loads(expected_result['message']))
        )
