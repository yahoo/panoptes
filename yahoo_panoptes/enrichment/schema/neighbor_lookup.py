"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module implements Neighbor Lookup EnrichmentGroup defined with schema validator
"""

from yahoo_panoptes.framework.enrichment import \
    PanoptesEnrichmentSchemaValidator, PanoptesEnrichmentGroup

BRIDGE_LOOKUP_SCHEMA_NAMESPACE = u'bridge_lookup'
INTERFACE_LOOKUP_SCHEMA_NAMESPACE = u'interface_lookup'
DEVICE_LOOKUP_SCHEMA_NAMESPACE = u'device_lookup'
L3_INTERFACE_LOOKUP_SCHEMA_NAMESPACE = u'l3_interface_lookup'
INVERSE_INTERFACE_LOOKUP_SCHEMA_NAMESPACE = u'inverse_interface_lookup'


class PanoptesBridgeLookupEnrichmentSchemaValidator(PanoptesEnrichmentSchemaValidator):
    schema = {
        'enrichment_label': {
            'type': 'dict',
            'schema': {
                'resource_id': {'type': 'string', 'required': True},
                'port_id_map': {'type': 'dict', 'required': True}
            }
        }
    }


class PanoptesInverseInterfaceLookupEnrichmentSchemaValidator(PanoptesEnrichmentSchemaValidator):
    schema = {
        'enrichment_label': {
            'type': 'dict',
            'schema': {
                'resource_id': {'type': 'string', 'required': True},
                'interface_description': {'type': 'string', 'required': True},
                'interface_index': {'type': 'string', 'required': True}
            }
        }
    }


class PanoptesL3InterfaceLookupEnrichmentSchemaValidator(PanoptesEnrichmentSchemaValidator):
    schema = {
        'enrichment_label': {
            'type': 'dict',
            'schema': {
                'resource_id': {'type': 'string', 'required': True},
                'interface_description': {'type': 'string', 'required': True},
                'interface_index': {'type': 'string', 'required': True},
                'ip_version': {'type': 'integer', 'required': True}
            }
        }
    }


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
                'ipv4': {'type': 'list', 'required': True},
                'ipv6': {'type': 'list', 'required': True},
                'source_resource_id': {'type': 'string', 'required': True}
            }
        }
    }


class PanoptesBridgeLookupEnrichmentGroup(PanoptesEnrichmentGroup):
    def __init__(self, enrichment_ttl, execute_frequency):
        super(PanoptesBridgeLookupEnrichmentGroup, self).__init__(
            namespace=BRIDGE_LOOKUP_SCHEMA_NAMESPACE,
            schema_validator=PanoptesBridgeLookupEnrichmentSchemaValidator(),
            enrichment_ttl=enrichment_ttl,
            execute_frequency=execute_frequency)


class PanoptesInverseInterfaceLookupEnrichmentGroup(PanoptesEnrichmentGroup):
    def __init__(self, enrichment_ttl, execute_frequency):
        super(PanoptesInverseInterfaceLookupEnrichmentGroup, self).__init__(
            namespace=INVERSE_INTERFACE_LOOKUP_SCHEMA_NAMESPACE,
            schema_validator=PanoptesInverseInterfaceLookupEnrichmentSchemaValidator(),
            enrichment_ttl=enrichment_ttl,
            execute_frequency=execute_frequency)


class PanoptesL3InterfaceLookupEnrichmentGroup(PanoptesEnrichmentGroup):
    def __init__(self, enrichment_ttl, execute_frequency):
        super(PanoptesL3InterfaceLookupEnrichmentGroup, self).__init__(
            namespace=L3_INTERFACE_LOOKUP_SCHEMA_NAMESPACE,
            schema_validator=PanoptesL3InterfaceLookupEnrichmentSchemaValidator(),
            enrichment_ttl=enrichment_ttl,
            execute_frequency=execute_frequency)


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
