"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module implements Interface EnrichmentGroup defined with schema validator
"""
from yahoo_panoptes.framework.enrichment import \
    PanoptesEnrichmentSchemaValidator, PanoptesEnrichmentGroup


INTERFACE_SCHEMA_NAMESPACE = u'interface'


class PanoptesInterfaceEnrichmentSchemaValidator(PanoptesEnrichmentSchemaValidator):
    schema = {
        u'enrichment_label': {
            u'type': u'dict',
            u'schema': {
                u'description': {u'type': u'string', u'required': True},
                u'media_type': {u'type': u'string', u'required': True},
                u'interface_name': {u'type': u'string', u'required': True},
                u'alias': {u'type': u'string', u'required': True},
                u'configured_speed': {u'type': u'integer', u'required': True},
                u'port_speed': {u'type': u'integer', u'required': True},
                u'parent_interface_name': {u'type': u'string', u'required': True},
                u'parent_interface_media_type': {u'type': u'string', u'required': True},
                u'parent_interface_configured_speed': {u'type': u'integer', u'required': True},
                u'parent_interface_port_speed': {u'type': u'integer', u'required': True},
                u'remote_device': {u'type': u'string', u'required': False},
                u'remote_device_site': {u'type': u'string', u'required': False},
                u'remote_device_function': {u'type': u'string', u'required': False},
                u'remote_device_purpose': {u'type': u'string', u'required': False},
                u'physical_address': {u'type': u'string', u'required': False}
            }
        }
    }


class PanoptesInterfaceEnrichmentGroup(PanoptesEnrichmentGroup):
    def __init__(self, enrichment_ttl, execute_frequency):
        super(PanoptesInterfaceEnrichmentGroup, self).__init__(
            namespace=INTERFACE_SCHEMA_NAMESPACE,
            schema_validator=PanoptesInterfaceEnrichmentSchemaValidator(),
            enrichment_ttl=enrichment_ttl,
            execute_frequency=execute_frequency)
