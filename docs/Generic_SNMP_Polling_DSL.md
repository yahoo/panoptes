# Generic SNMP Polling Plugin DSL Documentation

## Table of Contents
- [Background](#background)
- [Reference](#reference)
    - [Oids](#oids)
    - [Metrics Groups](#metrics-groups)

# Background
Before the introduction of the Generic SNMP Polling plugin, a plugin author would have to write two plugins to provide monitoring for any given resource: an enrichment plugin to collect enrichments particular to a specific device and a polling plugin that would consume those specific enrichments, and additionally poll for specific metrics.

After writing a few such plugins, we realized that instead we could write a single enrichment plugin that would output metrics and dimensions collected during the enrichment stage as well as instructions for what metrics to poll (and how to poll them) during the polling stage. This document explains the specifics of how to design an enrichment that will be processed by the Generic SNMP Polling Plugin.

# Reference
The schema for such enrichments that the Generic SNMP Polling Plugin requires is as follows:

```python
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
```

At its top level, and enrichment must define a set of oids and a set of metrics_groups. Each entry in oids is referenced by one or more entry in metrics_groups.

## Oids
The following keys are defined for each entry in the oids dictionary:


| Key | Acceptable Values | Required? |
| --- | ----------------- | -------- |
| method | "static", "get", "bulk_walk" | Yes |
| values | Dict mapping indices to values | Yes, if method is "static" |
| oid | String dot-delimited OID | Yes, if method is "get" or "bulk_walk" |
| index_transform | Dict mapping indices from **this** oid table to those used by the other (dimension or metric) oid tables. | |

According to these rules, the following are acceptable entries for oids:

```json
{
  "oids": {
    "cpu_name": {
      "method": "static",
      "values": {
        "1": "Switch System"
      }
    },
    "cpu_util": {
      "method": "bulk_walk",
      "oid": ".1.3.6.1.4.1.9.9.109.1.1.1.1.8"
    },
    "system_up_time": {
      "method": "get",
      "oid": ".1.3.6.1.2.1.1.3.0"
    },
    "memory_total": {
      "method": "static",
      "values": {
        "7.1.0.0": 2047868928,
        "9.1.0.0": 8154775552
      }
    }
  }
}
```

## Metrics Groups
The following keys are defined for each dictionary entry in the metrics_groups list:


| Key | Acceptable Values | Required? |
| --- | ----------------- | -------- |
| group_name | String specifying group name; Could be anything, but should adhere to standards. | Yes |
| dimensions | Dict mapping dimension names to values | No |
| metrics | Dict mapping metric names to values | Yes |

Take for example, the following simple use case:

```json
{
  "dimensions": {
    "memory_type": "memory_name.$index"
  },
  "group_name": "memory",
  "metrics": {
    "memory_total": {
      "metric_type": "gauge",
      "value": "memory_total.$index"
    },
    "memory_used": {
      "metric_type": "gauge",
      "value": "memory_used.$index"
    }
  }
}
```

#### group_name
The group name should be a simple, succinct description of what type of data can be found in each metrics group. In the above example, the group name is "memory" because the metrics group output by the Generic SNMP Polling Plugin will contain a dimension "memory_type" and metrics "memory_total" and "memory_used". Other example group names used in practice are "environment", "cpu", "status", "nat", "session", "interface", etc. While a group_name may in theory be any string, they should adhere to the standards found here.

#### dimensions and metrics
Both "dimensions" and "metrics" contain the same set of acceptable keys and values. The DSL defines the following keys:


| Key | Acceptable Values | Default Value? |
| --- | ---------------- | ---------- |
| type | ‘Integer’, ‘float’, ‘string’, and others | Corresponding type of value provided prior to applying a transform |
| metric_type | ‘Gauge’ or ‘counter’ | ‘gauge’ |
| value | Primitive type or string containing valid python expression or DSL expression | None, but see **Using the DSL Shorthand** below. |
| indices_from | String matching key in oids | |
| indices | List index strings, used by related table, to be queried| |
| transform | String containing python code to be evaluated by the Generic SNMP Polling Plugin | |

## Examples
The best way to understand this functionality is to step through a number of use cases. Consider the following trivial example:

#### Top-level Metric

```json
{
  "metrics_groups": [
    {
      "dimensions": {},
      "group_name": "environment",
      "metrics": {
        "fans_total": {
          "metric_type": "gauge",
          "type": "integer",
          "value": 1
        }
      }
    }
  ],
  "oids": {}
}
```

In this case, the only _metric_ we have assigned is "fans_total," which has a hard-coded value of "1". Our _group_name_ (by convention) is "environment", which encompasses metrics concerning fans, power units, and temperature. Finally, _dimensions_ is assigned to an empty dictionary, so each metric in _metrics_ refers to the device as a whole. (This makes intuitive sense, as we would expect the total number of fans to be reported "per device" rather than by any subdivided unit.) Because this metric refers to the device as a whole and does not have an associated dimension, in Panoptes parlance, we call this a "top level" metric.

Here, *oids* is assigned to an empty dictionary, because there are no values to look up; the _value_ for "fans_total" is hard-coded.


#### Simple Table Lookup

This enrichment defines a single entry for metrics_groups of _group_name_ "memory", and three entries for _oids_: "memory_name", "memory_total", and "memory_used." The syntax used for each "value" in each dimension and metric is the same:

<table name>.$index

This means, "get the value in <table name> for each index in <table name>." (The Bash-like ‘$’ denotes "index" as a DSL variable.)

For the dimension "memory_type," the Generic SNMP Polling Plugin will reference the oid dictionary named "memory_name". In this case, "memory_name" is statically defined (as oids used for dimensions will typically be), but could also be generated by the polling plugin at runtime (as is the case for "memory_used").

For each index in the table(s) reference in dimensions, a separate metrics group will be produced. Because "memory_name" contains only one key-value pair, the results generated by the Generic SNMP Polling PLugin contains only one metrics group:

```json
[
  {
    "metrics_group_interval": 60,
    "resource": {
      "resource_site": "test_site",
      "resource_id": "test_id",
      "resource_class": "network",
      "resource_plugin": "dummy",
      "resource_subclass": "test_subclass",
      "resource_endpoint": "127.0.0.1",
      "resource_metadata": {
        "model": "model",
        "_resource_ttl": "604800"
      },
      "resource_type": "test_type"},
    "dimensions": [
      {
        "dimension_name": "memory_type",
        "dimension_value": "Processor"
      }
    ],
    "metrics_group_type": "memory",
    "metrics": [
      {
        "metric_type": "gauge",
        "metric_name": "memory_used",
        "metric_value": 190000700
      },
      {
        "metric_type": "gauge",
        "metric_name": "memory_total",
        "metric_value": 1002273800
      }
    ],
    "metrics_group_schema_version": "0.2"
  }
]
```

#### Evaluated Value

Sometimes we need more to compute a value with more complexity than a simple table lookup. Thus "value"s may contain references to multiple tables in addition to python code of arbitrary complexity. Consider the following values:

```json
{
  "metrics_groups": [
    {
      "dimensions": {},
      "group_name": "environment",
      "metrics": {
        "fans_ok": {
          "metric_type": "gauge",
          "value": "len([x for x in fan_statuses.values() if x == '2'])"
        },
        "fans_total": 1
      }
    },
    {
      "dimensions": {},
      "group_name": "environment",
      "metrics": {
        "power_units_on": {
          "metric_type": "gauge",
          "value": "len([(x,y) for (x,y) in entity_fru_control.items() if x in power_supplies and y in ['2', '9', '12']])"
        },
        "power_units_total": 2
      }
    },
    {
      "dimensions": {
        "sensor": "temp_sensor_name.$index"
      },
      "group_name": "environment",
      "metrics": {
        "temperature_fahrenheit": {
          "indices_from": "temp_sensor_scales",
          "metric_type": "gauge",
          "transform": "lambda x: round((x * 1.8) + 32, 2)",
          "type": "float",
          "value": "int(ent_sensor_values.$index) * temp_sensor_scales.$index"
        }
      }
    }
  ]
}
```
This allows the user to write expressions tailored to the device data he is working with. For example, in the "value" expression for "power_units_on", `y in ['2', '9', '12']` is a check for status codes functionally equivalent to "on" for that specific type of device.

Note also that table lookups may need to be explicitly type-cast. In the above "value" for "temperature_fahrenheit", "ent_sensor_values.$index" would default to a "string" type, so we must explicitly cast it as an "int".

#### Simple Transform

```json
{
  "metrics_groups": [
    {
      "dimensions": {
        "sensor": "temp_sensor_name.$index"
      },
      "group_name": "environment",
      "metrics": {
        "temperature_fahrenheit": {
          "metric_type": "gauge",
          "transform": "lambda x: round((x * 1.8) + 32, 2) if x != 0 else 0.0",
          "type": "float",
          "value": "temp_sensor_values.$index"
        }
      }
    }
  ],
  "oids": {
    "temp_sensor_name": {
      "method": "static",
      "values": {
        "2.1.1.0": "Power Supply 0",
        "2.1.2.0": "Power Supply 1",
        "4.1.1.0": "Fan Tray 0",
        "4.1.2.0": "Fan Tray 1",
        "4.1.3.0": "Fan Tray 2",
        "7.1.0.0": "FPC: QFX10002-36Q @ 0/*/*",
        "8.1.1.0": "PIC: 36X40G @ 0/0/*",
        "9.1.0.0": "Routing Engine 0"
      }
    },
    "temp_sensor_values": {
      "method": "bulk_walk",
      "oid": ".1.3.6.1.4.1.2636.3.1.13.1.7"
    }
  }
}
```
Sometimes we want to perform a transformation on a value calculated or read from a table, such as when converting temperature values from Celsius to Fahrenheit as in the above enrichment. The value for the "transform" key is python code to be run by _eval_ in the Generic SNMP Polling Plugin for each _value_, in this case read in from "temp_sensor_values". This code rounds the value to two decimal places, and sets the output to _0.0_ if the value, _x_, is _0_.

Note the value of "type" refers to the type of the value passed as an argument to "transform", not the type of the final output (although in this case, both happen to be _float_.)

#### Report metrics for only certain indices

##### Using "indices"
In the previous example, 8 metrics groups would be reported -- one for each key-value pair in the dimension table, "temp_sensor_name" (provided all indices are also present in "temp_sensor_values"). If we are only interested in the temperature metrics for the power supplies, we could amend the "metrics_groups" definition as follows:

```json
{
  "metrics_groups": [
    {
      "dimensions": {
        "sensor": "temp_sensor_name.$index"
      },
      "group_name": "environment",
      "metrics": {
        "temperature_fahrenheit": {
          "metric_type": "gauge",
          "indices": [
            "2.1.1.0",
            "2.1.2.0"
          ],
          "transform": "lambda x: round((x * 1.8) + 32, 2) if x != 0 else 0.0",
          "type": "float",
          "value": "temp_sensor_values.$index"
        }
      }
    }
  ]
}
```

and our output metrics_groups would then be

```json
[
  {
    "metrics_group_interval": 60,
    "resource": {},
    "dimensions": [
      {
        "dimension_name": "sensor",
        "dimension_value": "Power Supply 0"
      }
    ],
    "metrics_group_type": "environment",
    "metrics": [
      {
        "metric_type": "gauge",
        "metric_name": "temperature_fahrenheit",
        "metric_value": 93.2
      }
    ],
    "metrics_group_schema_version": "0.2"
  },
  {
    "metrics_group_interval": 60,
    "resource": {},
    "dimensions": [
      {
        "dimension_name": "sensor",
        "dimension_value": "Power Supply 1"
      }
    ],
    "metrics_group_type": "environment",
    "metrics": [
      {
        "metric_type": "gauge",
        "metric_name": "temperature_fahrenheit",
        "metric_value": 94.1
      }
    ],
    "metrics_group_schema_version": "0.2"
  }
]
```

*Note: "resource" entries omitted for brevity.*

##### Using "indices_from"

Similarly, if we want to limit the metrics groups we output to indices found in a certain table, we can use the "indices_from" key. This is particularly useful when we are performing a calculation based upon data from two or more tables, where at least one's indices are a subset of the others'. Consider the prior example

```json
{
  "dimensions": {
    "sensor": {
      "value": "temp_sensor_name.$index"
    }
  },
  "group_name": "environment",
  "metrics": {
    "temperature_fahrenheit": {
      "indices_from": "temp_sensor_scales",
      "metric_type": "gauge",
      "transform": "lambda x: round((x * 1.8) + 32, 2)",
      "type": "float",
      "value": "int(ent_sensor_values.$index) * temp_sensor_scales.$index"
    }
  }
}
```

Here, the indices in "temp_sensor_scales" happen to be a subset of the indices in "ent_sensor_values" -- in this case because the latter contains values for all sensors in the device ("power" and "fan" in addition to "temperature"). **Note: If "indices_from" or "indices" is not present, the DSL will attempt to use indices for the first (i.e. left-most) table in the expression.** 

In this case, because "ent_sensor_values" contains indices not found in "temp_sensor_scales", the configuration would cause multiple exceptions were "indices_from" not specified. Here, we could fix this by re-ordering the table names in the "value" expression, but this will not always be possible, so it is better practice to be as explicit as possible with the configuration by using "indices_from".


#### Using "index_transform"

Sometimes the indices from one table will not match the indices used in another, even if the data in each are related. In these cases, it will be necessary to define an _index_transform_ in the _oids_ section. Take the following:

```json
{
  "metrics_groups": [
    {
      "dimensions": {
        "cpu_name": {
          "value": "cpu_name.$index"
        },
        "cpu_no": {
          "value": "cpu_no.$index"
        },
        "cpu_type": {
          "value": "'ctrl'"
        }
      },
      "group_name": "cpu",
      "metrics": {
        "cpu_utilization": {
          "metric_type": "gauge",
          "value": "cpu_util.$index"
        }
      }
    }
  ],
  "oids": {
    "cpu_name": {
      "method": "static",
      "values": {
        "22": "48X1GE, 4X10G Supervior in Fixed Module-1"
      }
    },
    "cpu_no": {
      "method": "static",
      "values": {
        "22": "Module 1"
      }
    },
    "cpu_util": {
      "index_transform": {
        "0": "26584",
        "1": "22",
        "10": "4959"
      },
      "method": "bulk_walk",
      "oid": ".1.3.6.1.4.1.9.9.109.1.1.1.1.8"
    }
  }
}
```

Here, the oid table for "cpu_util" uses the indices "0", "1", and "10", while the other oid tables contain only "22". The value for _.1.3.6.1.4.1.9.9.109.1.1.1.1.8.1_ will be polled and related to the values for index _22_ in "cpu_no" and "cpu_name". The output (omitting irrelevant key-value pairs) is:

```json
{
  "dimensions": [
    {
      "dimension_name": "cpu_name",
      "dimension_value": "48X1GE, 4X10G Supervior in Fixed Module-1"
    },
    {
      "dimension_name": "cpu_no",
      "dimension_value": "Module 1"
    },
    {
      "dimension_name": "cpu_type",
      "dimension_value": "ctrl"
    }
  ],
  "metrics_group_type": "cpu",
  "metrics": [
    {
      "metric_type": "gauge",
      "metric_name": "cpu_utilization",
      "metric_value": 5
    }
  ]
}
```

Note that because each metrics group is built using indices from the dimensions, the extraneous indices (here, "0" and "10") are simply ignored by the Generic SNMP Polling Plugin.

## Using the DSL Shorthand

Because the DSL is highly repetitive, it can be very redundant. Thus, there are some shorthand expressions to help reduce the length and complexity of enrichments. Let's start with a fully-expanded enrichment and make it more concise, step-by-step:

```json
{
  "metrics_groups": [
    {
      "dimensions": {
        "sensor": {
          "value": "temp_sensor_name.$index",
          "type": "string"
        }
      },
      "group_name": "environment",
      "metrics": {
        "temperature_fahrenheit": {
          "indices_from": "temp_sensor_scales",
          "metric_type": "gauge",
          "transform": "lambda x: round((x * 1.8) + 32, 2)",
          "type": "float",
          "value": "int(ent_sensor_values.$index) * temp_sensor_scales.$index"
        }
      }
    },
    {
      "dimensions": {},
      "group_name": "environment",
      "metrics": {
        "fans_ok": {
          "metric_type": "gauge",
          "type": "integer",
          "value": "len([x for x in cefc_fru_fan.values() if x == '2'])"
        },
        "fans_total": {
          "metric_type": "gauge",
          "type": "integer",
          "value": 1
        }
      }
    },
    {
      "dimensions": {},
      "group_name": "environment",
      "metrics": {
        "power_units_on": {
          "metric_type": "gauge",
          "type": "integer",
          "value": "len([(x,y) for (x,y) in entity_fru_control.items() if x in power_supplies and y in ['2', '9', '12']])"
        },
        "power_units_total": {
          "metric_type": "gauge",
          "type": "integer",
          "value": 2
        }
      }
    },
    {
      "dimensions": {
        "cpu_name": {
          "value": "cpu_name.$index",
          "type": "string"
        },
        "cpu_no": {
          "value": "cpu_no.$index",
          "type": "string"
        },
        "cpu_type": {
          "value": "'ctrl'",
          "type": "string"
        }
      },
      "group_name": "cpu",
      "metrics": {
        "cpu_utilization": {
          "metric_type": "gauge",
          "type": "integer",
          "value": "cpu_util.$index"
        }
      }
    },
    {
      "dimensions": {
        "memory_type": {
          "value": "memory_type.$index",
          "type": "string"
        }
      },
      "group_name": "memory",
      "metrics": {
        "memory_total": {
          "metric_type": "gauge",
          "type": "integer",
          "value": "memory_total.$index"
        },
        "memory_used": {
          "metric_type": "gauge",
          "type": "integer",
          "value": "memory_used.$index"
        }
      }
    }
  ]
}
```

As mentioned above, some of the keys in the "metrics" and "dimensions" sections have default values which apply the vast majority of the time (e.g. "metric_type" is almost always "gauge"). Removing these:

```json
{
  "metrics_groups": [
    {
      "dimensions": {
        "sensor": {
          "value": "temp_sensor_name.$index"
        }
      },
      "group_name": "environment",
      "metrics": {
        "temperature_fahrenheit": {
          "indices_from": "temp_sensor_scales",
          "transform": "lambda x: round((x * 1.8) + 32, 2)",
          "type": "float",
          "value": "int(ent_sensor_values.$index) * temp_sensor_scales.$index"
        }
      }
    },
    {
      "dimensions": {},
      "group_name": "environment",
      "metrics": {
        "fans_ok": {
          "value": "len([x for x in cefc_fru_fan.values() if x == '2'])"
        },
        "fans_total": {
          "value": 1
        }
      }
    },
    {
      "dimensions": {},
      "group_name": "environment",
      "metrics": {
        "power_units_on": {
          "value": "len([(x,y) for (x,y) in entity_fru_control.items() if x in power_supplies and y in ['2', '9', '12']])"
        },
        "power_units_total": {
          "value": 2
        }
      }
    },
    {
      "dimensions": {
        "cpu_name": {
          "value": "cpu_name.$index"
        },
        "cpu_no": {
          "value": "cpu_no.$index"
        },
        "cpu_type": {
          "value": "'ctrl'"
        }
      },
      "group_name": "cpu",
      "metrics": {
        "cpu_utilization": {
          "value": "cpu_util.$index"
        }
      }
    },
    {
      "dimensions": {
        "memory_type": {
          "value": "memory_type.$index"
        }
      },
      "group_name": "memory",
      "metrics": {
        "memory_total": {
          "value": "memory_total.$index"
        },
        "memory_used": {
          "value": "memory_used.$index"
        }
      }
    }
  ]
}
```

We can reduce the length of the enrichment definition even more by taking note of the many entries which now contain only a "value" key-value pair. For example:

```json
{
  "memory_used": {
    "value": "memory_used.$index"
  }
}
```

If "value" is the only key present, we can use the following shorthand which maps the _metric_name_ or _dimension_name_ directly to the value of "value". Here:

```json
{
  "memory_used": "memory_used.$index"
}
```

Much cleaner.

Applied to the full example, we have:

```json
{
  "metrics_groups": [
    {
      "dimensions": {
        "sensor": "temp_sensor_name.$index"
      },
      "group_name": "environment",
      "metrics": {
        "temperature_fahrenheit": {
          "indices_from": "temp_sensor_scales",
          "transform": "lambda x: round((x * 1.8) + 32, 2)",
          "type": "float",
          "value": "int(ent_sensor_values.$index) * temp_sensor_scales.$index"
        }
      }
    },
    {
      "dimensions": {},
      "group_name": "environment",
      "metrics": {
        "fans_ok": "len([x for x in cefc_fru_fan.values() if x == '2'])",
        "fans_total": 1
      }
    },
    {
      "dimensions": {},
      "group_name": "environment",
      "metrics": {
        "power_units_on": "len([(x,y) for (x,y) in entity_fru_control.items() if x in power_supplies and y in ['2', '9', '12']])",
        "power_units_total": 2
      }
    },
    {
      "dimensions": {
        "cpu_name": "cpu_name.$index",
        "cpu_no": "cpu_no.$index",
        "cpu_type": "'ctrl'"
      },
      "group_name": "cpu",
      "metrics": {
        "cpu_utilization": "cpu_util.$index"
      }
    },
    {
      "dimensions": {
        "memory_type": "memory_type.$index"
      },
      "group_name": "memory",
      "metrics": {
        "memory_total": "memory_total.$index",
        "memory_used": "memory_used.$index"
      }
    }
  ]
}
```

In this case, the fully-detailed version of _metrics_groups_ contains 97 lines, while the condensed version has only 53 -- a 45% reduction. This goes a long way toward improving readability and accelerating plugin development when a user wants to quickly report a few metrics and dimensions for a resource.