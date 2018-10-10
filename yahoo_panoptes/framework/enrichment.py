"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module defines Panoptes enrichment and their related abstractions
"""

import copy
import json
import time

from cerberus import Validator

from . import const
from .context import PanoptesContext
from .exceptions import PanoptesBaseException
from .resources import PanoptesResource
from .utilities.key_value_store import PanoptesKeyValueStore
from .validators import PanoptesValidators


class PanoptesEnrichmentException(PanoptesBaseException):
    """
    The base class for all Panoptes Enrichment exceptions
    """
    pass


class PanoptesEnrichmentCacheKeyValueStore(PanoptesKeyValueStore):
    """
    A custom Key/Value store for Panoptes Enrichment Cache
    """
    redis_group = const.ENRICHMENT_REDIS_GROUP

    def __init__(self, panoptes_context):
        super(PanoptesEnrichmentCacheKeyValueStore, self).__init__(
            panoptes_context, const.ENRICHMENT_PLUGIN_RESULTS_KEY_VALUE_NAMESPACE)


class PanoptesEnrichmentSchemaValidator(object):
    """
    Schema validator base class based on Cerberus
    """
    schema = {'key': 'value'}

    def __init__(self):
        assert isinstance(self.schema, dict) and len(self.schema) > 0, 'schema must be a non empty Cerberus schema dict'
        self.__cerberus_validator = Validator(schema=self.schema)

    def validate(self, enrichment_set_data):
        """
        Validates PanoptesEnrichmentSet object against defined schema

        Args:
            enrichment_set_data (PanoptesEnrichmentSet): PanoptesEnrichmentSet to validate
        Returns:
              bool
        """
        assert isinstance(enrichment_set_data, PanoptesEnrichmentSet), \
            'element set must be an instance of PanoptesEnrichmentSet'
        schema = copy.deepcopy(self.schema)
        schema[enrichment_set_data.key] = schema.pop('enrichment_label')
        return self.__cerberus_validator.validate(document=enrichment_set_data._raw_data, schema=schema)


class PanoptesEnrichmentEncoder(json.JSONEncoder):
    """
    Custom Json encoder to convert set to list during encoding
    """
    # https://github.com/PyCQA/pylint/issues/414
    def default(self, o):  # pylint: disable=E0202
        if isinstance(o, set):
            return list(o)
        if isinstance(o, PanoptesResource):
            return o.__dict__['_PanoptesResource__data']
        if isinstance(o, PanoptesEnrichmentSet):
            return o.__dict__['_PanoptesEnrichmentSet__data']
        if isinstance(o, PanoptesEnrichmentGroup):
            return o.__dict__['_PanoptesEnrichmentGroup__data']
        if isinstance(o, PanoptesEnrichmentGroupSet):
            return o.__dict__['_PanoptesEnrichmentGroupSet__data']
        if isinstance(o, PanoptesEnrichmentMultiGroupSet):
            return o.__dict__['_PanoptesEnrichmentMultiGroupSet__data']
        return json.JSONEncoder.default(self, o)


class PanoptesEnrichmentSet(object):
    """
    Representation of a basic enrichment element

    A PanoptesEnrichmentSet hold key(label) and collection of enrichment info as its value

    Args:
        key (as defined in schema): label for enrichment element collection
        value (dict): enrichment collection

    Examples:
        An example enrichment set data structure

    {
       "int_001" : {
          "speed" : 1000,
          "index" : 1,
          "status" : "up"
       }
    }
    """
    def __init__(self, key, value={None: None}):
        assert PanoptesValidators().valid_nonempty_string(key), 'enrichment key must be a string'
        assert isinstance(value, dict), 'enrichment value must be a dict'
        if value == {None: None}:
            value = {}
        self.__data = dict()
        self._key = key
        self.__data[self._key] = value

    @property
    def key(self):
        return self._key

    @property
    def value(self):
        return self.__data[self._key]

    @property
    def _raw_data(self):
        return self.__data

    def add(self, enrichment_key, enrichment_value):
        """
        Adds enrichment key and value elements to PanoptesEnrichmentSet object

        Args:
            enrichment_key (as defined in schema): The enrichment element key
            enrichment_value (as defined in schema): The enrichment element value corresponding to the key

        Returns:
            None

        """
        self.__data[self._key][enrichment_key] = enrichment_value

    def json(self):
        return json.dumps(self.__data, sort_keys=True)

    def __repr__(self):
        return self.__class__.__name__ + '({0})'.format(str(self.__data))

    def __hash__(self):
        return hash(self._key)

    def __len__(self):
        return len(self.__data[self._key])

    def __eq__(self, other):
        if not isinstance(other, PanoptesEnrichmentSet):
            return False
        return self._key == other._key


class PanoptesEnrichmentGroup(object):
    """
    Representation of Enrichment Group

    Collection of enrichment elements(PanoptesEnrichmentSet) grouped by Enrichment key(namespace)

    Args:
        namespace (str): enrichment namespace
        schema_validator (PanoptesEnrichmentSchemaValidator): PanoptesEnrichmentSchemaValidator
        instance initialized with schema
        execute_frequency (int): Execute frequency of the plugin which produced this object
        enrichment_ttl (int): TTL value of the enrichment object

    Examples:
        An example PanoptesEnrichmentGroup data structure

    {
       "namespace" : "interface",
       "data" : [
          {
             "value" : {
                "speed" : 1000,
                "status" : "up",
                "index" : 1
             },
             "key" : "int_001"
          }
       ]
    }
    """

    def __init__(self, namespace, schema_validator, enrichment_ttl, execute_frequency):
        assert PanoptesValidators().valid_nonempty_string(namespace), 'enrichment namespace must be a string'
        assert isinstance(schema_validator, PanoptesEnrichmentSchemaValidator), \
            'schema_validator must be an instance of PanoptesEnrichmentSchemaValidator'
        assert PanoptesValidators().valid_nonzero_integer(enrichment_ttl), \
            'enrichment_ttl must be a valid nonzero integer'
        assert PanoptesValidators().valid_nonzero_integer(execute_frequency), \
            'execute_frequency must be a valid nonzero integer'
        self.__data = dict()
        self.__data['metadata'] = dict()
        self.__data['namespace'] = namespace
        self.__data['data'] = set()
        self.__schema_validator = schema_validator
        self.__data['metadata']['_enrichment_group_creation_timestamp'] = time.time()
        self.__data['metadata']['_enrichment_ttl'] = enrichment_ttl
        self.__data['metadata']['_execute_frequency'] = execute_frequency

    @property
    def enrichment_schema(self):
        return self.__schema_validator.schema

    @property
    def namespace(self):
        return self.__data['namespace']

    @property
    def data(self):
        return self.__data['data']

    @property
    def _raw_data(self):
        return self.__data

    @property
    def metadata(self):
        return self.__data['metadata']

    @property
    def enrichment_ttl(self):
        return self.__data['metadata']['_enrichment_ttl']

    @property
    def execute_frequency(self):
        return self.__data['metadata']['_execute_frequency']

    @property
    def enrichment_group_creation_timestamp(self):
        return self.__data['metadata']['_enrichment_group_creation_timestamp']

    def add_enrichment_set(self, enrichment_set):
        """
        Adds enrichment_set(PanoptesEnrichmentSet) to create Enrichment Group

        Args:
            enrichment_set (PanoptesEnrichmentSet): Collection of enrichment elements(PanoptesEnrichmentSet)

        Returns:
            None
        """
        assert isinstance(enrichment_set, PanoptesEnrichmentSet), \
            'enrichment set must be an instance of PanoptesEnrichmentSet'

        assert self.__schema_validator.validate(enrichment_set), \
            'schema validation failed for enrichment_set data'
        self.__data['data'].discard(enrichment_set)
        self.__data['data'].add(enrichment_set)

    def upsert_metadata(self, metadata_key, metadata_value):
        """
        Adds metadata key / value pairs to PanoptesEnrichmentGroup

        Args:
            metadata_key (str): metadata key
            metadata_value (str or int or float):  metadata value

        Returns:
              None
        """
        assert PanoptesValidators().valid_nonempty_string(metadata_key), 'metadata_key must be a string'

        if metadata_key.startswith('_'):
            raise ValueError('Failed to update reserved metadata')

        assert \
            PanoptesValidators().valid_nonempty_string(metadata_value) or \
            PanoptesValidators().valid_number(metadata_value), 'metadata_value must be any of string / float / integer'

        self.__data['metadata'][metadata_key] = metadata_value

    def bulk_add_enrichment_set(self):
        pass

    def json(self):
        return json.dumps(self.__data, sort_keys=True, cls=PanoptesEnrichmentEncoder)

    def serialize_data(self):
        return json.dumps({enrichment_set.key: enrichment_set.value for enrichment_set in self.data}, sort_keys=True)

    def serialize(self):
        enrichment_serialize = dict()
        enrichment_serialize['data'] = {enrichment_set.key: enrichment_set.value for enrichment_set in self.data}
        enrichment_serialize['metadata'] = self.__data['metadata']
        return json.dumps(enrichment_serialize, sort_keys=True)

    def __repr__(self):
        return self.__class__.__name__ + '({0})'.format(str(self.__data))

    def __len__(self):
        return len(self.__data['data'])

    def __hash__(self):
        return hash(self.namespace)

    def __eq__(self, other):
        if not isinstance(other, PanoptesEnrichmentGroup):
            return False
        return self.namespace == other.namespace


class PanoptesEnrichmentGroupSet(object):
    """
    Representation of Enrichment Group Set

    Collection of enrichment group grouped by resource

    Args:
        resource (PanoptesResource): PanoptesResource object

    Example:
        An example PanoptesEnrichmentGroupSet data structure
    {
       "resource" : PanoptesResource_instance,
       "enrichment" : [
          {
             "namespace" : "interface",
             "data" : [
                {
                   "value" : {
                      "index" : 1,
                      "status" : "up",
                      "speed" : 1000
                   },
                   "key" : "int_001"
                }
             ]
          }
       ]
    }
    """

    def __init__(self, resource):
        assert isinstance(resource, PanoptesResource), 'resource must be an instance of PanoptesResource'
        self.__data = dict()
        self.__data['resource'] = resource
        self.__data['enrichment_group_set_creation_timestamp'] = time.time()
        self.__data['enrichment'] = set()

    @property
    def resource(self):
        return self.__data['resource']

    @property
    def enrichment(self):
        return self.__data['enrichment']

    @property
    def enrichment_group_set_creation_timestamp(self):
        return self.__data['enrichment_group_set_creation_timestamp']

    @property
    def _raw_data(self):
        return self.__data

    def add_enrichment_group(self, enrichment_group):
        """
        Adds enrichment_group(PanoptesEnrichmentGroup) to create enrichment group set

        Args:
            enrichment_group (PanoptesEnrichmentGroup): Enrichment elements grouped by key

        Returns:
            None
        """
        assert isinstance(enrichment_group, PanoptesEnrichmentGroup), \
            'enrichment_group must be an instance of PanoptesEnrichmentGroup'
        assert len(enrichment_group.data) > 0, 'enrichment_group must hold at least one data set'
        self.__data['enrichment'].discard(enrichment_group)
        self.__data['enrichment'].add(enrichment_group)

    def bulk_add_enrichment_group(self):
        pass

    def json(self):
        return json.dumps(self.__data, sort_keys=True, cls=PanoptesEnrichmentEncoder)

    def __repr__(self):
        return self.__class__.__name__ + '({0})'.format(str(self.__data))

    def __len__(self):
        return len(self.__data['enrichment'])

    def __hash__(self):
        namespaces_self = ''.join(sorted([item.namespace for item in self.enrichment]))
        return hash(self.resource.resource_id + namespaces_self)

    def __eq__(self, other):
        if not isinstance(other, PanoptesEnrichmentGroupSet):
            return False
        namespaces_other = ''.join(sorted([item.namespace for item in other.enrichment]))
        namespaces_self = ''.join(sorted([item.namespace for item in self.enrichment]))
        return self.resource.resource_id == other.resource.resource_id and namespaces_self == namespaces_other


class PanoptesEnrichmentMultiGroupSet(object):
    """
    Collection of PanoptesEnrichmentGroupSet belongs to multiple Panoptes resources
    """
    def __init__(self):
        self.__data = dict()
        self.__data['group_sets'] = set()

    def add_enrichment_group_set(self, enrichment_group_set):
        """
        Adds enrichment_group_set(PanoptesEnrichmentGroupSet)

        Args:
            enrichment_group_set (PanoptesEnrichmentGroupSet): Enrichment group set of a Panoptes resource

        Returns:
            None
        """
        assert isinstance(enrichment_group_set, PanoptesEnrichmentGroupSet), \
            'enrichment_group_set must be an instance of PanoptesEnrichmentGroupSet'
        assert len(enrichment_group_set) > 0, 'enrichment_group_set must hold at least one data set'
        self.__data['group_sets'].discard(enrichment_group_set)
        self.__data['group_sets'].add(enrichment_group_set)

    @property
    def enrichment_group_sets(self):
        return self.__data['group_sets']

    def __len__(self):
        return len(self.__data['group_sets'])

    def __repr__(self):
        return self.__class__.__name__ + '({0})'.format(str(self.__data))

    def json(self):
        return json.dumps(self.__data, sort_keys=True, cls=PanoptesEnrichmentEncoder)


class PanoptesEnrichmentCacheError(PanoptesBaseException):
    """
    The base class for all Panoptes Enrichment Cache errors
    """
    pass


class PanoptesEnrichmentCache(object):
    """
    Fetches enrichment from kv store and act as cache for polling plugins

    Args:
        panoptes_context (PanoptesContext): The PanoptesContext being used by the Plugin Agent
        plugin_conf (dict): Plugin conf dict
        resource (PanoptesResource): Resource object associated with plugin runner instance
    """

    def __init__(self, panoptes_context, plugin_conf, resource):
        assert isinstance(panoptes_context, PanoptesContext), 'panoptes_context must be an instance of PanoptesContext'
        assert isinstance(resource, PanoptesResource), 'resource must be an instance of PanoptesResource'
        assert isinstance(plugin_conf, dict), 'plugin_conf value must be a dict'

        self._logger = panoptes_context.logger
        self._plugin_name = plugin_conf['Core']['name']
        self._panoptes_context = panoptes_context
        self._enrichment_conf = plugin_conf['enrichment']
        self._resource_id = resource.resource_id
        self._preload_conf = self._parse_conf()
        self._enrichment_data = dict()

        try:
            self._kv_store = PanoptesEnrichmentCacheKeyValueStore(panoptes_context)
        except Exception as e:
            raise e

        self._process_enrichment()

        self._logger.debug('Successfully created PanoptesEnrichmentCache enrichment_data {} for plugin {}'.
                           format(self._enrichment_data, self._plugin_name))

    def get_enrichment(self, resource_id, namespace):
        """
        Returns enrichment data for matching resource_id and namespace

        Args:
            resource_id (str): Panoptes resource id
            namespace (str): Enrichment namespace
        Returns:
              enrichment_data (dict): enrichment_data dict
        """
        assert PanoptesValidators().valid_nonempty_string(resource_id), 'resource_id must be a string'
        assert PanoptesValidators().valid_nonempty_string(namespace), 'enrichment namespace must be a string'

        if resource_id == 'self':
            resource_id = self._resource_id

        try:
            return {key: value for key, value in self._enrichment_data[resource_id][namespace].items()}
        except KeyError:
            try:
                self._preload_data(resource_id, namespace)
                return {key: value for key, value in self._enrichment_data[resource_id][namespace].items()}
            except Exception as e:
                raise PanoptesEnrichmentCacheError('Failed to get data for resource {} namespace {} from enrichment '
                                                   'resource object: {}'.format(resource_id, namespace, repr(e)))

    def get_enrichment_value(self, resource_id, namespace, enrichment_key):
        """
        Returns enrichment value for matching resource_id, namespace and
        enrichment_key

        Args:
            resource_id (str): Panoptes resource id
            namespace (str): Enrichment namespace
            enrichment_key (str): Enrichment set key
        Returns:
              enrichment_set (dict): key, value pairs of enrichment
        """
        assert PanoptesValidators().valid_nonempty_string(resource_id), 'resource_id must be a string'
        assert PanoptesValidators().valid_nonempty_string(namespace), 'enrichment namespace must be a string'
        assert PanoptesValidators().valid_nonempty_string(enrichment_key), 'enrichment_key must be a string'

        if resource_id == 'self':
            resource_id = self._resource_id

        try:
            return self._enrichment_data[resource_id][namespace][enrichment_key]
        except KeyError:
            try:
                self._preload_data(resource_id, namespace)
                return self._enrichment_data[resource_id][namespace][enrichment_key]
            except Exception as e:
                raise PanoptesEnrichmentCacheError(
                    'Failed to get data for resource {} namespace {} enrichment_key {} '
                    'from enrichment cache object: {}'.format(resource_id, namespace, enrichment_key, repr(e)))

    def get_enrichment_keys(self, resource_id, namespace):
        """
        Returns enrichment keys for matching resource_id and namespace

        Args:
            resource_id (str): Panoptes resource id
            namespace (str): Enrichment namespace
        Returns:
              enrichment_keys (list): enrichment keys(label)
        """
        assert PanoptesValidators().valid_nonempty_string(resource_id), 'resource_id must be a string'
        assert PanoptesValidators().valid_nonempty_string(namespace), 'enrichment namespace must be a string'

        if resource_id == 'self':
            resource_id = self._resource_id

        try:
            return self._enrichment_data[resource_id][namespace].keys()
        except KeyError:
            try:
                self._preload_data(resource_id, namespace)
                return self._enrichment_data[resource_id][namespace].keys()
            except Exception as e:
                raise PanoptesEnrichmentCacheError('Failed to get data for resource {} namespace {} from enrichment '
                                                   'cache object: {}'.format(resource_id, namespace, repr(e)))

    def _process_enrichment(self):
        for resource, namespace in self._preload_conf:
            if resource == 'self':
                resource = self._resource_id

            namespace_keys = None
            if namespace == '*':
                try:
                    namespace_keys = self._kv_store.find_keys(resource + const.KV_NAMESPACE_DELIMITER + namespace)
                except Exception as e:
                    self._logger.error(
                        'Error while scanning namespace pattern {} on KV store for plugin {} resource {}: {}'.format(
                            namespace, self._plugin_name, resource, repr(e)))
                if namespace_keys:
                    namespaces = [namespace_field.split(':')[-1] for namespace_field in namespace_keys]
                    for normalized_namespace in namespaces:
                        self._preload_data(resource, normalized_namespace)
            else:
                self._preload_data(resource, namespace)

    def _preload_data(self, resource, namespace):
        try:
            key = resource + const.KV_NAMESPACE_DELIMITER + namespace
            value = self._kv_store.get(key)
            if value:
                data = json.loads(value).get('data')
                self._enrichment_data.setdefault(resource, dict())
                self._enrichment_data[resource].update(**{namespace: data})
                self._logger.debug('Successfully populated enrichment for plugin {} resource {} namespace {} data {}'.
                                   format(self._plugin_name, resource, namespace, data))
            else:
                self._logger.error(
                    'No enrichment data found on KV store for plugin {} resource {} namespace {} using key {}'.format(
                        self._plugin_name, resource, namespace, key))
        except Exception as e:
            raise IOError(
                'Failed while pre-loading enrichment from KV store for plugin {} resource {} namespace {}: {}'.format(
                    self._plugin_name, resource, namespace, repr(e)))

    def _parse_conf(self):
        try:
            return {(item.split(':')[0].strip(), item.split(':')[1].strip())
                    for item in self._enrichment_conf.get('preload').split(',')}
        except Exception as e:
            raise ('Failed while parsing preload enrichment configuration from plugin conf for '
                   'plugin {}: {}'.format(self._plugin_name, repr(e)))
