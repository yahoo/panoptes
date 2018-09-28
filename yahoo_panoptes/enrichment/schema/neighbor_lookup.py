"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module implements Neighbor Lookup EnrichmentGroup defined with schema validator
"""

from ...framework.enrichment import PanoptesEnrichmentSchemaValidator, PanoptesEnrichmentGroup

INTERFACE_LOOKUP_SCHEMA_NAMESPACE = 'interface_lookup'
DEVICE_LOOKUP_SCHEMA_NAMESPACE = 'device_lookup'


class PanoptesInterfaceLookupEnrichmentSchemaValidator(PanoptesEnrichmentSchemaValidator):
    schema = {
        'enrichment_label': {
            'type': 'dict',
            'schema': {
                'resource_id': {'type': 'string', 'required': True},
                'interface_description': {'type': 'string', 'required': True},
                'interface_index': {'type': 'string', 'required': True},
                'port_id_map': {'type': 'dict', 'required': True}
            }
        }
    }


class PanoptesDeviceLookupEnrichmentSchemaValidator(PanoptesEnrichmentSchemaValidator):
    schema = {
        'enrichment_label': {
            'type': 'dict',
            'schema': {
                'device_info_ipv4': {'type': 'dict', 'required': True},
                'device_info_ipv6': {'type': 'dict', 'required': True},
                'source_resource_id': {'type': 'string', 'required': True}
            }
        }
    }


class PanoptesInterfaceLookupEnrichmentGroup(PanoptesEnrichmentGroup):
    def __init__(self, enrichment_ttl, execute_frequency):
        super(PanoptesInterfaceLookupEnrichmentGroup, self).__init__(
            namespace=INTERFACE_LOOKUP_SCHEMA_NAMESPACE,
            schema_validator=PanoptesInterfaceLookupEnrichmentSchemaValidator(),
            enrichment_ttl=enrichment_ttl,
            execute_frequency=execute_frequency)


class PanoptesDeviceLookupEnrichmentGroup(PanoptesEnrichmentGroup):
    def __init__(self, enrichment_ttl, execute_frequency):
        super(PanoptesDeviceLookupEnrichmentGroup, self).__init__(
            namespace=DEVICE_LOOKUP_SCHEMA_NAMESPACE,
            schema_validator=PanoptesDeviceLookupEnrichmentSchemaValidator(),
            enrichment_ttl=enrichment_ttl,
            execute_frequency=execute_frequency)
