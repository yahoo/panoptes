"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module implements Interface EnrichmentGroup defined with schema validator
"""

from ...framework.enrichment import PanoptesEnrichmentSchemaValidator, PanoptesEnrichmentGroup

INTERFACE_SCHEMA_NAMESPACE = 'interface'


class PanoptesInterfaceEnrichmentSchemaValidator(PanoptesEnrichmentSchemaValidator):
    schema = {
        'enrichment_label': {
            'type': 'dict',
            'schema': {
                'description': {'type': 'string', 'required': True},
                'media_type': {'type': 'string', 'required': True},
                'interface_name': {'type': 'string', 'required': True},
                'alias': {'type': 'string', 'required': True},
                'configured_speed': {'type': 'integer', 'required': True},
                'port_speed': {'type': 'integer', 'required': True},
                'parent_interface_name': {'type': 'string', 'required': True},
                'parent_interface_media_type': {'type': 'string', 'required': True},
                'parent_interface_configured_speed': {'type': 'integer', 'required': True},
                'parent_interface_port_speed': {'type': 'integer', 'required': True},
                'remote_device': {'type': 'string', 'required': False},
                'remote_device_site': {'type': 'string', 'required': False},
                'remote_device_function': {'type': 'string', 'required': False},
                'remote_device_purpose': {'type': 'string', 'required': False},
                'physical_address': {'type': 'string', 'required': False}
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
