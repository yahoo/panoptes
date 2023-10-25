from builtins import range
from builtins import object
import copy
import json
import logging
import os
import subprocess
import re

from unittest.mock import Mock, patch, create_autospec
from yahoo_panoptes.framework.context import PanoptesContext
from yahoo_panoptes.framework.enrichment import PanoptesEnrichmentCache, PanoptesEnrichmentCacheKeyValueStore
from yahoo_panoptes.framework.plugins.context import PanoptesPluginWithEnrichmentContext, PanoptesPluginContext
from yahoo_panoptes.plugins.polling.utilities.polling_status import DEVICE_METRICS_STATES
from yahoo_panoptes.framework.resources import PanoptesResource
from yahoo_panoptes.framework.utilities.helpers import ordered
from yahoo_panoptes.framework.utilities.secrets import PanoptesSecretsStore

from tests.mock_redis import PanoptesMockRedis

mock_time = Mock()
mock_time.return_value = 1512629517.03121

snmp_simulator = None


def setup_module_default(plugin_pwd, snmp_sim_listen=u'127.0.0.1:10161', snmp_sim_data_dir=u'data/recording'):
    global snmp_simulator

    snmp_sim_data_dir = os.path.join(plugin_pwd, snmp_sim_data_dir)
    try:
        snmp_simulator = subprocess.Popen(
                ['snmpsimd.py',
                 '--data-dir={}'.format(snmp_sim_data_dir),
                 '--agent-udpv4-endpoint={}'.format(snmp_sim_listen),
                 '--logging-method=null'],
        )
    except Exception as e:
        raise IOError('Failed to start SNMP simulator: {}'.format(repr(e)))


def tear_down_module_default():
    try:
        if snmp_simulator is not None:
            snmp_simulator.kill()
    except OSError:
        pass


class DiscoveryPluginTestFramework(object):
    plugin = None
    path = None

    results_data_file = 'results.json'

    plugin_conf = {
        'Core': {
            'name': 'Test Plugin',
            'module': 'test_plugin'
        },
        'main': {
            'execute_frequency': '60',
            'enrichment_ttl': '300',
            'resource_filter': 'resource_class = "network"'
        }
    }

    def __init__(self, test_name):
        super(DiscoveryPluginTestFramework, self).__init__(test_name)

        self._path = self.path
        self._plugin_conf = self.plugin_conf

        self._expected_results = None
        self._results_data_file = 'data/' + self.results_data_file
        self._plugin_context = None
        self._panoptes_context = None

    def set_panoptes_context(self):
        panoptes_test_conf_file = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                                               'config_files/test_panoptes_config.ini')

        self._panoptes_context = PanoptesContext(
                panoptes_test_conf_file,
                key_value_store_class_list=[PanoptesEnrichmentCacheKeyValueStore]
        )

    def set_plugin_context(self):
        self._plugin_context = create_autospec(
                PanoptesPluginContext, instance=True, spec_set=True,
                config=self._plugin_conf,
                logger=logging.getLogger(__name__)
        )

    def set_expected_results(self):
        self._results_data_file = 'data/' + self.results_data_file
        expected_result_file = os.path.join(os.path.abspath(self.path), self._results_data_file)
        self._expected_results = json.load(open(expected_result_file))

    @patch('time.time', mock_time)
    @patch('yahoo_panoptes.framework.resources.time', mock_time)
    def setUp(self):
        self.set_panoptes_context()
        self.set_plugin_context()
        self.set_expected_results()


class SNMPPluginTestFramework(object):
    plugin_class = None
    path = None

    snmp_host = u'127.0.0.1'
    snmp_port = 10161
    snmp_timeout = 10
    snmp_retries = 1
    snmp_max_repetitions = 10
    snmp_community = 'public'
    snmp_failure_timeout = 1

    x509_secured_requests = 0
    x509_key_location = u'/home/panoptes/x509/keys'
    x509_key_filename = u'panoptes.key'
    x509_cert_location = u'/home/panoptes/x509/keys'
    x509_cert_filename = u'panoptes.pem'

    resource_id = u'test_id'
    resource_endpoint = u'127.0.0.1'
    resource_plugin = u'dummy'
    resource_site = u'test_site'
    resource_class = u'network'
    resource_subclass = u'test_subclass'
    resource_type = u'test_type'
    resource_backplane = None
    resource_model = u'model'

    results_data_file = u'results.json'
    enrichment_data_file = u'enrichment_data'
    use_enrichment = True

    plugin_conf = {
        'Core': {
            'name': 'Test Plugin',
            'module': 'test_plugin'
        },
        'main': {
            'execute_frequency': '60',
            'enrichment_ttl': '300',
            'resource_filter': 'resource_class = "network"'
        },
        'snmp': {
            'timeout': 10,
            'retries': 1,
            'non_repeaters': 1,
            'max_repetitions': 25,
        },
        'x509': {
            'x509_secured_requests': 0,
            'x509_key_location': '/home/panoptes/x509/keys',
            'x509_key_filename': 'panoptes.key',
            'x509_cert_location': '/home/panoptes/x509/keys',
            'x509_cert_filename': 'panoptes.pem'
        }
    }

    def __init__(self, test_name):
        super(SNMPPluginTestFramework, self).__init__(test_name)
        self._path = self.path
        self._plugin_conf = self.plugin_conf

        self._snmp_host = self.snmp_host
        self._snmp_port = self.snmp_port
        self._snmp_timeout = self.snmp_timeout
        self._snmp_retries = self.snmp_retries
        self._snmp_max_repetitions = self.snmp_max_repetitions
        self._snmp_community = self.snmp_community
        self._snmp_failure_timeout = self.snmp_failure_timeout

        self._x509_secured_requests = self.x509_secured_requests
        self._x509_cert_location = self.x509_cert_location
        self._x509_cert_filename = self.x509_cert_filename
        self._x509_key_location = self.x509_key_location
        self._x509_key_filename = self.x509_key_filename

        self._resource_id = self.resource_id
        self._resource_endpoint = self.resource_endpoint
        self._resource_plugin = self.resource_plugin
        self._resource_site = self.resource_site
        self._resource_class = self.resource_class
        self._resource_subclass = self.resource_subclass
        self._resource_type = self.resource_type
        self._resource_backplane = self.resource_backplane
        self._resource_model = self.resource_model

        self._panoptes_context = None
        self._panoptes_resource = None

        self._snmp_conf = None
        self._x509_conf = None
        self._expected_results = None
        self._results_data_file = None
        self._enrichment_data_file = None
        self._secret_store = None
        self._plugin_context = None
        self._plugin_context_bad = None
        self._snmp_conf_bad = None
        self._x509_conf_bad = None
        self._enrichment_kv = None
        self._enrichment_cache = None

    def set_panoptes_context(self):
        panoptes_test_conf_file = os.path.join(os.path.dirname(os.path.dirname(__file__)),
                                               'config_files/test_panoptes_config.ini')

        self._panoptes_context = PanoptesContext(
                panoptes_test_conf_file,
                key_value_store_class_list=[PanoptesEnrichmentCacheKeyValueStore]
        )

    def set_panoptes_resource(self):
        self._panoptes_resource = PanoptesResource(
                resource_site=self._resource_site,
                resource_class=self._resource_class,
                resource_subclass=self._resource_subclass,
                resource_type=self._resource_type,
                resource_id=self._resource_id,
                resource_endpoint=self._resource_endpoint,
                resource_plugin=self._resource_plugin
        )

        self._panoptes_resource.resource_metadata['model'] = self._resource_model

        if self._resource_backplane:
            self._panoptes_resource.add_metadata('backplane', self._resource_backplane)

    def set_enrichment_cache(self, resource_endpoint=None):
        if self.use_enrichment:
            self._enrichment_data_file = 'data/' + self.enrichment_data_file
            self._enrichment_kv = self._panoptes_context.get_kv_store(PanoptesEnrichmentCacheKeyValueStore)
            enrichment_data_file = os.path.join(os.path.abspath(self.path), self._enrichment_data_file)

            if self._plugin_conf.get('enrichment'):
                if self._plugin_conf['enrichment'].get('preload'):
                    self._enrichment_cache = PanoptesEnrichmentCache(
                            self._panoptes_context,
                            self._plugin_conf,
                            self._panoptes_resource
                    )

                try:
                    with open(enrichment_data_file) as enrichment_data:
                        for line in enrichment_data:
                            key, value = line.strip().split('=>')
                            if resource_endpoint is not None:
                                value = self.update_enrichment_ip(value, resource_endpoint)
                            self._enrichment_kv.set(key, value)
                except Exception as e:
                    raise IOError('Failed to load enrichment data file {}: {}'.format(enrichment_data_file, repr(e)))

    def set_snmp_conf(self):
        self._snmp_conf = self._panoptes_context.config_object.snmp_defaults

    def set_x509_conf(self):
        self._x509_conf = {
            'x509_secured_requests': self._x509_secured_requests,
            'x509_cert_location': self._x509_cert_location,
            'x509_cert_filename': self._x509_cert_filename,
            'x509_key_location': self._x509_key_location,
            'x509_key_filename': self._x509_key_filename
        }

    def set_secret_store(self):
        self._secret_store = create_autospec(PanoptesSecretsStore, instance=True, spec_set=True)
        self._secret_store.get_by_site.return_value = self._snmp_community

    def set_plugin_context(self):
        self._plugin_context = create_autospec(
                PanoptesPluginWithEnrichmentContext, instance=True, spec_set=True,
                data=self._panoptes_resource,
                enrichment=self._enrichment_cache,
                config=self._plugin_conf,
                snmp=self._snmp_conf,
                x509=self._x509_conf,
                secrets=self._secret_store,
                logger=logging.getLogger(__name__)
        )

    def set_x509_conf_bad(self):
        self._x509_conf_bad = copy.copy(self._x509_conf)
        self._x509_conf_bad['x509_secured_requests'] = 5

    def set_snmp_conf_bad(self):
        self._snmp_conf_bad = copy.copy(self._snmp_conf)
        self._snmp_conf_bad['port'] += 1
        self._snmp_conf_bad['timeout'] = self._snmp_failure_timeout

    def set_plugin_context_bad(self):
        self._plugin_context_bad = create_autospec(
                PanoptesPluginWithEnrichmentContext, instance=True, spec_set=True,
                data=self._panoptes_resource,
                enrichment=self._enrichment_cache,
                config=self._plugin_conf,
                snmp=self._snmp_conf_bad,
                x509=self._x509_conf,
                secrets=self._secret_store,
                logger=logging.getLogger(__name__)
        )

    def set_expected_results(self):
        self._results_data_file = 'data/' + self.results_data_file
        expected_result_file = os.path.join(os.path.abspath(self.path), self._results_data_file)
        self._expected_results = json.load(open(expected_result_file))

    def update_enrichment_ip(self, enrichment, resource_endpoint):
        """
        The PluginPollingGenericSNMPMetrics Plugin initially calls the function _get_config() when started.
        This function loads the enrichment from the PanoptesEnrichmentCacheKeyValueStore
        using the resource_id, namespace, and enrichment key. The enrichment key is the resource_endpoint.
        no_service_active tests to ensure that a proper error (timeout -> ping error) is returned when
        snmp_bulk is called with an invalid IP. In order to get to this stage in the tests the enrichment must
        loaded in the generic snmp runner. In order to test this, the resource_endpoint is changed to
        be invalid. However this IP must also be changed in the test enrichment file so the generic snmp runner
        is able to load the enrichment from the cache.
        """
        modified_enrichment = json.loads(enrichment)

        if len(modified_enrichment) == 0 or len(list(modified_enrichment['data'].keys())) == 0:
            return enrichment

        ip = list(modified_enrichment['data'].keys())[0]

        if re.search(r'\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b', ip):
            modified_enrichment['data'][resource_endpoint] = modified_enrichment['data'][ip]
            del modified_enrichment['data'][ip]

            return json.dumps(modified_enrichment)

        return enrichment

    @patch('time.time', mock_time)
    @patch('yahoo_panoptes.framework.resources.time', mock_time)
    @patch('redis.StrictRedis', PanoptesMockRedis)
    def setUp(self):
        self.set_panoptes_context()
        self.set_panoptes_resource()
        self.set_enrichment_cache()
        self.set_snmp_conf()
        self.set_x509_conf()
        self.set_secret_store()
        self.set_plugin_context()
        self.set_snmp_conf_bad()
        self.set_x509_conf_bad()
        self.set_plugin_context_bad()
        self.set_expected_results()


class SNMPEnrichmentPluginTestFramework(SNMPPluginTestFramework):
    @patch('time.time', mock_time)
    @patch('yahoo_panoptes.framework.resources.time', mock_time)
    def test_enrichment_plugin_results(self):
        """Test plugin result and validate results with input data/results.json"""
        plugin = self.plugin_class()
        result = plugin.run(self._plugin_context)

        self.assertIsNotNone(result)

        result = ordered(json.loads(result.json()))
        expected = ordered(self._expected_results)

        self.assertEqual(result, expected)

    def test_enrichment_plugin_timeout(self):
        """Test plugin raises error during SNMP timeout"""
        plugin = self.plugin_class()

        with self.assertRaises(Exception):
            plugin.run(self._plugin_context_bad)


class SNMPPollingPluginTestFramework(SNMPPluginTestFramework):
    plugin_metrics_function = 'get_device_metrics'
    uses_polling_status = True

    @staticmethod
    def _remove_timestamps(results):
        metrics_group_json_strings = list()

        for metrics_group in results.metrics_groups:

            metrics_group_dict = json.loads(metrics_group.json)

            resource_dict = metrics_group_dict['resource']
            resource_dict.pop('resource_creation_timestamp')

            for metric in metrics_group_dict['metrics']:
                metric.pop('metric_creation_timestamp')

            metrics_group_dict.pop('metrics_group_creation_timestamp')
            metrics_group_dict['resource'] = resource_dict

            metrics_group_json_strings.append(metrics_group_dict)

        return metrics_group_json_strings

    def set_expected_results(self):
        self._results_data_file = 'data/' + self.results_data_file
        self._expected_results = list()
        expected_result_file = os.path.join(os.path.abspath(self.path), self._results_data_file)
        with open(expected_result_file) as results_file:
            expected_results = json.load(results_file)
            for result in expected_results:
                self._expected_results.append(result)

    @patch('time.time', mock_time)
    @patch('yahoo_panoptes.framework.resources.time', mock_time)
    @patch('yahoo_panoptes.framework.metrics.time', mock_time)
    def test_basic_operations(self):

        plugin = self.plugin_class()
        results = plugin.run(self._plugin_context)

        self.assertEqual(ordered(self._expected_results), ordered(self._remove_timestamps(results)))

    def test_invalid_resource_endpoint(self):
        self._resource_endpoint = u'127.0.0.257'
        self._snmp_conf['timeout'] = self._snmp_failure_timeout
        self.set_panoptes_resource()
        self.set_plugin_context()

        plugin = self.plugin_class()
        results = plugin.run(self._plugin_context)

        if self.uses_polling_status is True:
            self.assertEqual(len(results.metrics_groups), 1)
        else:
            self.assertEqual(len(results.metrics_groups), 0)

        self._resource_endpoint = u'127.0.0.1'
        self._snmp_conf['timeout'] = self.snmp_timeout
        self.set_panoptes_resource()
        self.set_plugin_context()

    def test_inactive_port(self):
        """Tests a valid resource_endpoint with an inactive port"""
        self.plugin_conf['snmp']['port'] = 10161
        self._results_data_file = 'data/bad_port_results.json'
        self.set_expected_results()

        plugin = self.plugin_class()
        results = plugin.run(self._plugin_context_bad)

        self.assertEqual(ordered(self._expected_results), ordered(self._remove_timestamps(results)))

        self._results_data_file = 'data/results.json'

    def test_no_service_active(self):
        """Tests a valid resource_endpoint with no service active"""
        self._resource_endpoint = u'192.0.2.1'  # RFC 5737
        self.set_enrichment_cache(resource_endpoint=self._resource_endpoint)

        self._snmp_conf['timeout'] = self._snmp_failure_timeout
        self.set_panoptes_resource()
        self.set_plugin_context()

        plugin = self.plugin_class()

        if self._plugin_conf.get('enrichment'):
            if self._plugin_conf['enrichment'].get('preload'):
                result = plugin.run(self._plugin_context)
                result = self._remove_timestamps(result)

                ping_failure = False
                for i in range(len(result)):
                    if len(result[i]['metrics']) >= 1 and result[i]['metrics'][0]['metric_value'] \
                            == DEVICE_METRICS_STATES.PING_FAILURE:
                        ping_failure = True

                self.assertTrue(ping_failure)

        self._resource_endpoint = u'127.0.0.1'
        self._snmp_conf['timeout'] = self.snmp_timeout
        self.set_panoptes_resource()
        self.set_plugin_context()

    def test_null_metrics_exceptions(self):
        """Test plugin behaves correctly when metrics are absent"""
        mock_get_device_metrics = Mock(return_value=None)

        plugin = self.plugin_class()
        with patch('.'.join(
                [plugin.__module__, self.plugin_class.__name__, self.plugin_metrics_function]),
                   mock_get_device_metrics):
            results = plugin.run(self._plugin_context)

            if results is not None:
                raise AssertionError('results is not None, it is: {}'.format(results))
