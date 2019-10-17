"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module contains constants used throughout the Panoptes system
"""

# System Wide Constants
import signal
# <CR> The SIGNALS_TO_NAMES_DICT code comes from Python Standard Library By Example
SIGNALS_TO_NAMES_DICT = dict((getattr(signal, n), n)
                             for n in dir(signal) if n.startswith('SIG') and u'_' not in n)
KEY_VALUE_NAMESPACE_PREFIX = u'panoptes:'
DEFAULT_ROOT_LOGGER_NAME = u'panoptes'
DEFAULT_LOG_FORMAT = u'[%(asctime)s: %(levelname)s/%(processName)s] %(message)s'
CELERY_LOADER_MODULE = u'celery.utils.imports'
DEFAULT_REDIS_GROUP_NAME = u'default'
CELERY_REDIS_GROUP_NAME = u'celery'
KV_STORE_DELIMITER = u'|'
KV_STORE_SCAN_ITER_COUNT = 1000
KV_NAMESPACE_DELIMITER = u':'
LOCK_PATH_DELIMITER = u'/'

# Configuration Manager Related Constants
CONFIG_FILE_ENVIRONMENT_VARIABLE = u'PANOPTES_CONFIG_FILE'
DEFAULT_CONFIG_FILE_PATH = u'/home/panoptes/conf/panoptes.ini'

# Plugin Scheduler Related Constants
PLUGIN_TYPES = [u'discovery', u'polling', u'enrichment']
PLUGIN_EXTENSION = u'panoptes-plugin'
PLUGINS_KEY_VALUE_NAMESPACE = KEY_VALUE_NAMESPACE_PREFIX + u'plugins_kv'
PLUGIN_SCHEDULER_MAX_CYCLES_WITHOUT_LOCK = 5
PLUGIN_SCHEDULER_LOCK_PATH = u'/panoptes/plugin_scheduler'
PLUGIN_CLIENT_ID_PREFIX = u'plugin'

# Plugin Agent Related Constants
PLUGINS_METADATA_KEY_VALUE_NAMESPACE = KEY_VALUE_NAMESPACE_PREFIX + u'plugins_metadata'
PLUGIN_AGENT_LOCK_PATH = u'/panoptes/plugin_agent'
PLUGIN_AGENT_LOCK_ACQUIRE_TIMEOUT = 5
PLUGIN_AGENT_PLUGIN_TIMESTAMPS_EXPIRE = 604800
PLUGIN_AGENT_PLUGIN_EXPIRES_MULTIPLE = 2
PLUGIN_AGENT_PLUGIN_TIME_LIMIT_MULTIPLE = 1.25

# Secrets Manager Related Constants
SECRETS_MANAGER_KEY_VALUE_NAMESPACE = KEY_VALUE_NAMESPACE_PREFIX + u'secrets'

# Discovery Manager Related Constants
DISCOVERY_PLUGIN_SCHEDULER_KEY_VALUE_NAMESPACE = KEY_VALUE_NAMESPACE_PREFIX + u'discovery_plugin_scheduler'
DISCOVERY_PLUGIN_SCHEDULER_CELERY_APP_NAME = u'discovery_plugin_scheduler'
DISCOVERY_PLUGIN_SCHEDULER_CELERY_TASK_PREFIX = u'discovery_plugin_scheduler'
DISCOVERY_PLUGIN_SCHEDULER_LOCK_ACQUIRE_TIMEOUT = 5

# Discovery Plugin Agent Related Constants
DISCOVERY_PLUGIN_AGENT_KEY_VALUE_NAMESPACE = KEY_VALUE_NAMESPACE_PREFIX + u'discovery_plugin_agent_kv'
DISCOVERY_PLUGIN_AGENT_MODULE_NAME = u'yahoo_panoptes.discovery.discovery_plugin_agent.discovery_plugin_task'
DISCOVERY_PLUGIN_AGENT_CELERY_APP_NAME = u'discovery_plugin_agent'
DISCOVERY_PLUGIN_AGENT_PLUGINS_CHILD_LOGGER_NAME = u'discovery_plugins'
DISCOVERY_PLUGIN_AGENT_LOCK_PATH = u'/panoptes/discovery/plugin_agent/plugins/lock'
DISCOVERY_PLUGIN_AGENT_LOCK_ACQUIRE_TIMEOUT = 5

# Resource Cache Related Constants
RESOURCE_CACHE_DB_CURSOR_SIZE = 1000
RESOURCE_CACHE_UPDATE_INTERVAL = 300

# Resource Manager Related Constants
RESOURCE_MANAGER_REDIS_GROUP = u'resources'
RESOURCE_MANAGER_KEY_VALUE_NAMESPACE = KEY_VALUE_NAMESPACE_PREFIX + u'resource_manager_kv:resource'
RESOURCE_MANAGER_CLIENT_ID_PREFIX = u'resource_manager'
RESOURCE_MANAGER_KAFKA_GROUP_ID = u'resource_manager_group'
RESOURCE_MANAGER_KAFKA_POLL_TIMEOUT = 15
RESOURCE_MANAGER_RESOURCE_EXPIRE = 604800
RESOURCE_MANAGER_MAX_PARTITION_FETCH_BYTES = 10485760

# Polling Plugin Scheduler Related Constants
POLLING_PLUGIN_SCHEDULER_KEY_VALUE_NAMESPACE = KEY_VALUE_NAMESPACE_PREFIX + u'polling_plugin_scheduler_kv'
POLLING_PLUGIN_SCHEDULER_LOCK_ACQUIRE_TIMEOUT = 5
POLLING_PLUGIN_SCHEDULER_CELERY_APP_NAME = u'polling_plugin_scheduler'
POLLING_PLUGIN_SCHEDULER_CELERY_TASK_PREFIX = u'polling_plugin_task'

# Enrichment Plugin Scheduler Related Constants
ENRICHMENT_PLUGIN_SCHEDULER_KEY_VALUE_NAMESPACE = KEY_VALUE_NAMESPACE_PREFIX + u'enrichment_plugin_scheduler_kv'
ENRICHMENT_PLUGIN_SCHEDULER_LOCK_ACQUIRE_TIMEOUT = 5
ENRICHMENT_PLUGIN_SCHEDULER_CELERY_APP_NAME = u'enrichment_plugin_scheduler'
ENRICHMENT_PLUGIN_SCHEDULER_CELERY_TASK_PREFIX = u'enrichment_plugin_task'

# Polling Plugin Agent Related Constants
POLLING_PLUGIN_AGENT_KEY_VALUE_NAMESPACE = KEY_VALUE_NAMESPACE_PREFIX + u'polling_plugin_agent_kv'
POLLING_PLUGIN_AGENT_MODULE_NAME = u'yahoo_panoptes.polling.polling_plugin_agent.polling_plugin_task'
POLLING_PLUGIN_AGENT_CELERY_APP_NAME = u'polling_plugin_agent'
POLLING_PLUGIN_AGENT_PLUGINS_CHILD_LOGGER_NAME = u'polling_plugins'

# Enrichment Plugin Agent Related Constants
ENRICHMENT_REDIS_GROUP = u'enrichments'
ENRICHMENT_PLUGIN_AGENT_KEY_VALUE_NAMESPACE = KEY_VALUE_NAMESPACE_PREFIX + u'enrichment_plugin_agent_kv'
ENRICHMENT_PLUGIN_AGENT_MODULE_NAME = u'yahoo_panoptes.enrichment.enrichment_plugin_agent.enrichment_plugin_task'
ENRICHMENT_PLUGIN_AGENT_CELERY_APP_NAME = u'enrichment_plugin_agent'
ENRICHMENT_PLUGIN_AGENT_PLUGINS_CHILD_LOGGER_NAME = u'enrichment_plugins'

# Enrichment Plugins Results Related Constants
ENRICHMENT_PLUGIN_RESULTS_KEY_VALUE_NAMESPACE = PLUGINS_KEY_VALUE_NAMESPACE + KV_NAMESPACE_DELIMITER + u'enrichment'

# Metrics processing constants
METRICS_CLIENT_ID_PREFIX = u'metrics_processing'
METRICS_KAFKA_GROUP_ID = u'metrics_processing_group'
METRICS_KAFKA_POLL_TIMEOUT = 5
METRICS_KEY_VALUE_NAMESPACE = KEY_VALUE_NAMESPACE_PREFIX + u'metrics_kv'
METRICS_CONFIDENCE_THRESHOLD = 0.33
METRICS_KV_STORE_TTL = 14400
METRICS_KV_STORE_TTL_MULTIPLE = 3

# Metrics topics related constants
METRICS_RAW_TOPIC_SUFFIX = u'metrics'
METRICS_PROCESSED_TOPIC_SUFFIX = u'processed'
METRICS_TOPIC_NAME_DELIMITER = u'-'
METRICS_TOPIC_KEY_DELIMITER = u':'
METRICS_REDIS_GROUP = u'metrics'

# Materials Science related constants
MELTING_POINT_STEEL = 1371
