## Table of Contents

- [Objects And Abstractions](#objects-and-abstractions)
  - [Site](#site)
  - [Resource](#resource)
    - [Serialization](#serialization)
    - [DSL](#dsl)
  - [Enrichment](#enrichment)
  - [Metrics](#metrics)
    - [Metrics Group](#metrics-group)
    - [Metrics Group Type](#metrics-group-type)
    - [Dimensions](#dimensions)
    - [Timestamps](#timestamps)
    - [Time Series](#time-series)
    - [Serialization](#serialization)
- [Plugins](#plugins)
  - [Configuration And Code](#configuration-and-code)
    - [Configuration](#configuration)
    - [Code](#code)
  - [Scheduler](#scheduler)
  - [Agent](#agent)
  - [Security](#security)
- [Subsystems](#subsystems)
  - [Discovery](#discovery)
  - [Enrichment](#enrichment)
  - [Polling](#polling)
  - [Consumers](#consumers)

## Objects and Abstractions

### Site

A 'site' is a string which identifies a physical or logical collection of [resources](#resource) to monitor. At Oath, 
we set the site to the name of datacenter in which a Panoptes instance is hosted.

### Resource

A 'resource' in Panoptes is an abstraction of something that should be monitored. Typically, it would be a network 
device with associated metadata. The key properties a resource has are:

* A system wide unique id
* The site to which this resource belongs
* An 'endpoint' which can be monitored - in the case of network devices, this would be the hostname
* Three classifiers - class, subclass and type
* The name of the plugin that discovered this resource
* The Unix timestamp of creation of the resource
* The TTL (in seconds) of the resource
* Associated metadata, which are string based key value pairs

Resources are 'discovered' by the [Discovery subsystem](#discovery).

At Oath, we set the resource ids to FQDNs for network devices since they are guaranteed to be unique within our
system. Also, we set the following classifiers:

* Class: "network" for network devices, "system" for hosts
* Subclass: Typically set to the _function_ of the resource - e.g. switch, firewall, router
* Type: For network devices, the manufacturer name

Note that the above are an implementation detail at Oath and only meant to serve as guidance - in your installation,
they could be set to any value.

We would urge you to consider your resource naming and classification standards before a large scale rollout -
as you will see in the next section, setting up resources correctly makes operations easier.

#### Serialization

Resources are encapsulated by the PanoptesResource class and are serialized as JSON. An example 
resource looks like:

```json
{
    "resource_id": "switch1.fqdn",
    "resource_site": "dc1",
    "resource_endpoint": "switch.fqdn",
    "resource_class": "network",
    "resource_subclass": "switch",
    "resource_type": "cisco",
    "resource_creation_timestamp": "1532023147.34",
    "resource_creation_plugin": "switch_discovery_plugin",
    "resource_metadata": {
        "_resource_ttl": "604800",
        "make": "Cisco",
        "model": "4948E",
        "os_name": "Cisco IOS",
        "os_version": "15.1(2)SG3"
    }
}
```
#### DSL

A powerful concept throughout Panoptes is of the 'resource filter DSL' which can be applied to select/filter resources 
within various subsystems. This DSL is a subset of SQL with the following operators supported:

*  =, !=, eq, ne, LIKE, AND, OR, NOT, IN

An example resource filter would look like:

```sql
resource_class = "network"
AND resource_subclass = "switch"
AND resource_type != "arista" 
AND resource_endpoint IN ("test1","test2")
AND resource_metadata.make NOT LIKE "Arista%"
AND resource_metadata.model NOT IN ("model1", "model2")
```

The above query would select all resources which:
* Are classified as network switches
* Are not of type arista or have their make set to Arista
* Are not named 'test1' or 'test2'
* Are not models 'model1' or 'model2'

Note that grouping by parenthesis 'e.g. NOT (resource_site = "dc1" OR resource_site = "dc2")' is **not** currently
supported.

The resource DSL is used in enrichment and polling plugin configurations.

### Enrichment

Enrichments are metadata associated with any element within Panoptes. Enrichments may be collected from resources
directly, OR by parsing/processing the telemetry collected from a resource OR by looking up an entirely different
resource.

Enrichments have the following properties:

* Can only be of type string
* Change less frequently than [metrics](#metrics)
* Usually are more expensive to process than metrics

Because of all the properties mentioned above, enrichments cached in Redis and available to be used by any subsystem.

Take, for example, the interface names from a network device. They don't change frequently and thus can be looked up
in-frequently (say once every 30 minutes) and cached.

### Metrics

Panoptes, as telemetry system, primarily collects, transforms and processes _metrics_ from resources. A metric is,
simply put, a number which can be measured and plotted. For example, the 'bits in' on an interface is a metric that can
measured.

Metrics are grouped and annotated in couple of ways within Panoptes:

#### Metric Type

Panoptes currently supports two types of metrics: counters and gauges.

#### Metrics Group

A set of metrics is called a metrics group within Panoptes.

#### Metrics Group Type
A set of related metrics are grouped by a 'metrics group type'. Take, for example, metrics related to interfaces - 
bits in, bits out, packets in, packets out etc. - these are collectively grouped under the metric group type 'interface' 
in Panoptes.

Note that this grouping is by 'contract only' - that is, plugin authors are encouraged to get metrics groups with the
same name if they adhere to a common schema. Nothing in Panoptes actually enforces this.

#### Dimensions

In addition to the numbers in a metric group type, it is pertinent to provide some metadata about what the metrics
specifically refer to. In the interface metrics group type example, providing the name of the interface, in addition to
just the metrics would make sense.

These metadata are called 'dimensions' in Panoptes and they are arbitrary string based key/value pairs. By default, for
each metric group type, the resource it's associated with is added to the metrics group. Commonly, enrichments collected
for a resource would also be looked up and added to metrics groups.

Some telemetry and monitoring systems refer to these as 'tags'.

#### Timestamps

Each metric in Panoptes has an associated millisecond resolution Unix epoch timestamp. For polled metrics (ala SNMP),
this is the timestamp when a plugin received the metric from a resource - not when it was sampled/created on the
resource.

#### Time Series

A set of unique dimension keys _and_ values, in addition to a metric name uniquely identifies a time series.

Continuing with the interface metrics example, the following could potentially identify a unique time series:

* Resource Endpoint (e.g. switch1.fqdn)
* Interface Name (e.g. eth0)
* Metric Name (e.g. bits_in)

#### Serialization

Metrics are serialized as JSON within Panoptes. A sample serialization looks as follows:

```json
{
  "resource":{
    "resource_id": "switch1.fqdn",
    "resource_site": "dc1",
    "resource_endpoint": "switch.fqdn",
    "resource_class": "network",
    "resource_subclass": "switch",
    "resource_type": "cisco",
    "resource_creation_timestamp": 1532023147.34,
    "resource_creation_plugin": "switch_discovery_plugin",
    "resource_metadata": {
        "_resource_ttl": 604800,
        "make": "Cisco",
        "model": "4948E",
        "os_name": "Cisco IOS",
        "os_version": "15.1(2)SG3"
    }
  },
  "dimensions": [
    {
      "dimension_name": "interface_name",
      "dimension_value": "GigabitEthernet0"
    }
  ],
  "metrics": [
    {
      "metric_creation_timestamp": 1532023709.743,
      "metric_type": "counter",
      "metric_name": "bits_in",
      "metric_value": 7445623452378547
    }
  ],
  "metrics_group_type": "interface",
  "metrics_group_schema_version": "0.2",
  "metrics_group_creation_timestamp": 1532023709.732,
  "metrics_group_interval": 60
}
```

## Plugins

Plugins in Panoptes are the key to it's extensibility. In a nutshell, plugins are Python classes which implement 
well-known interfaces and produce well-known Python objects as results relevant to the type of plugin.

### Configuration And Code

Plugins are defined by two files: a configuration file and a file that contains the actual code of the plugin.

#### Configuration
The configuration file for a plugin is an ini style file which contains, amongst other things, the location of the code
file. The default extension for plugin configuration files is ```.panoptes-plugin``` - although this can be changed from 
the site wide configuration file. Example plugin configuration files can be found in the
[examples/plugins](https://github.com/yahoo/panoptes/tree/master/examples/plugins) directory of the main git repo.

```[Core]``` and ```[Documentation]``` are mandatory sections in a plugin configuration file. In addition, the 
```exequte_frequency``` in the ```[main]``` section of a plugin configuration file is mandatory as well - this is the 
time, in seconds, on how frequently the plugin should be executed.

#### Code

The plugin configuration file _must_ contain the fully qualified path for the plugin module in ```[Core]``` section.
This module should contain at least one class derived from one of the three plugin type classes
(PanoptesDiscoveryPlugin, PanoptesEnrichmentPlugin, PanoptesPollingPlugin).

If may not be obvious from the preceding descriptions, but multiple plugin configuration files can point to the same
module but with different plugin configs - for example, the resource_filter could be different for each plugin
configuration.

### Scheduler

Plugins are discovered by a 'plugin scheduler' - it scans the directories listed in the site wide configuration file 
for plugin configuration files and if the plugins can be successfully loaded (no configuration or code errors),
then it's schedule the plugins to be executed. 'Scheduling' a plugin means that the task is added to Celery on the
appropriate queue and with the appropriate arguments.

### Agent

A plugin agent is a process which actually runs plugins - it spawns multiple sub-processes, watches the Celery queue
for the next task to execute, loads and runs a plugin and handles the plugins' output appropriately.

A plugin agent implements a couple of safety guards:

* It takes global distributed lock on plugin/resource combination before execution - so if the same plugin is running
against the same resource, another instance of the plugin would not be run against the same resource
* It does not execute a plugin which was executed less than execute_frequency ago against the same resource

Both of these are to ensure that resources are not 'hammered' in any way - this is specially needed for network devices
which can be DOS'ed unintentionally by over-polling.

### Security

Since plugins are Python code that can do _anything_ that the user which the plugin agent is running as is capable of
and they run in the context of shared processes, it is extremely important to review any plugins before deciding to run
them in your setup.

## Subsystems

### Discovery

Discovery is the process of enumerating resources to be used throughout the rest of the system. A discovery plugin
interacts with any external system (for e.g. a CMDB) and produces a set of resources, with relevant metadata, which can
be used by the rest of the system.

The base Panoptes package contains a discovery plugin that can read a JSON file for a list of resources and add them
to the system.

### Enrichment

The enrichment subsystem in Panoptes schedules and executes enrichment plugins and upserts their output into Redis.

### Polling

Polling is the act of actually collecting metrics. As mentioned above, polling plugins (like all other plugins) are
Python code and thus can collect from any source - SNMP, API, CLI.

#### Transformations

The polling agent has the ability to apply transforms to telemetry collected by polling plugins - this is done before
the data is placed on the message bus. Currently, there is only one implemented transformation - conversion from counter
to gauge. This is immensely useful for network telemetry since a lot of devices return only counters.

#### Message Bus

The polling agent places collected and transformed metrics onto topics on the Kafka bus.

### Consumers

Consumers take the data produced by the discovery, enrichment or polling subsystems and integrate them with external
systems. The base Panoptes package ships with a consumer to transform metrics and emit metrics to InfluxDB.
