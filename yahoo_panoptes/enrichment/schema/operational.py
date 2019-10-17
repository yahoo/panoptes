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
        u'enrichment_label': {
            u'type': u'dict',
            u'schema': {
                u'snmpenginetime': {u'type': u'integer', u'required': False},  # epoch seconds since last restart
                u'sysdescr': {u'type': u'string', u'required': False},
                u'device_vendor': {u'type': u'string', u'required': False},
                u'device_model': {u'type': u'string', u'required': False},
                u'device_os': {u'type': u'string', u'required': False},
                u'device_os_version': {u'type': u'string', u'required': False},
                # yahoo_panoptes.plugins.polling.utilities.polling_status.DEVICE_METRICS_STATES
                u'device_polling_status': {u'type': u'integer', u'required': True},
                u'last_updated': {u'type': u'integer', u'required': False},  # epoch seconds since last update
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
