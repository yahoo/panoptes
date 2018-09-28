"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.


This module implements Metrics EnrichmentGroup defined with schema validator
"""

from ....framework.enrichment import PanoptesEnrichmentSchemaValidator, PanoptesEnrichmentGroup


class PanoptesGenericSNMPMetricsEnrichmentSchemaValidator(PanoptesEnrichmentSchemaValidator):
    schema = {
        'enrichment_label': {
            'type': 'dict',
            'schema': {
                'oids': {
                    'type': 'dict', 'required': True
                },
                'metrics_groups': {
                    'type': 'list',
                    'required': True,
                    'schema': {
                        'type': 'dict',
                        'schema': {
                            'group_name': {
                                'type': 'string',
                                'required': True
                            },
                            'dimensions': {
                                'type': 'dict', 'required': False
                            },
                            'metrics': {
                                'type': 'dict', 'required': True
                            }
                        }
                    }
                }
            }
        }
    }


class PanoptesGenericSNMPMetricsEnrichmentGroup(PanoptesEnrichmentGroup):
    METRICS_SCHEMA_NAMESPACE = 'metrics'

    def __init__(self, enrichment_ttl, execute_frequency):
        super(PanoptesGenericSNMPMetricsEnrichmentGroup, self).__init__(
            namespace=self.METRICS_SCHEMA_NAMESPACE,
            schema_validator=PanoptesGenericSNMPMetricsEnrichmentSchemaValidator(),
            enrichment_ttl=enrichment_ttl,
            execute_frequency=execute_frequency)
