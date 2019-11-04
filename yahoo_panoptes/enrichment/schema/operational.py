"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module implements OperationalEnrichmentGroup defined with schema validator
"""

from yahoo_panoptes.framework.enrichment import \
    PanoptesEnrichmentSchemaValidator, PanoptesEnrichmentGroup

OPERATIONAL_SCHEMA_NAMESPACE = u'operational'


class PanoptesOperationalEnrichmentSchemaValidator(PanoptesEnrichmentSchemaValidator):
    schema = {
        'enrichment_label': {
            'type': 'dict',
            'schema': {
                'snmpenginetime': {'type': 'integer', 'required': False},  # epoch seconds since last restart
                'sysdescr': {'type': 'string', 'required': False},
                'device_vendor': {'type': 'string', 'required': False},
                'device_model': {'type': 'string', 'required': False},
                'device_os': {'type': 'string', 'required': False},
                'device_os_version': {'type': 'string', 'required': False},
                # yahoo_panoptes.plugins.polling.utilities.polling_status.DEVICE_METRICS_STATES
                'device_polling_status': {'type': 'integer', 'required': True},
                'last_updated': {'type': 'integer', 'required': False},  # epoch seconds since last update
            }
        }
    }


class PanoptesOperationalEnrichmentGroup(PanoptesEnrichmentGroup):
    def __init__(self, enrichment_ttl, execute_frequency):
        super(PanoptesOperationalEnrichmentGroup, self).__init__(
            namespace=OPERATIONAL_SCHEMA_NAMESPACE,
            schema_validator=PanoptesOperationalEnrichmentSchemaValidator(),
            enrichment_ttl=enrichment_ttl,
            execute_frequency=execute_frequency)
