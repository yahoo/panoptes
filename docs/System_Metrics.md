# Background

This document defines conventions that we've determined for Panoptes.  This formalizes the approach to 
normalizing the varied returns from different devices into a single set of enriched metrics.
We recommend the following structures to maintain consistency.

## Table of Contents

- [Data Classes](#data-classes)
- [Device Polling Status](#device-polling-status)
- [Plugin Execution Flow](#plugin-execution-flow)

# Data Classes

Metrics come in two flavors, **counters** and **gauges**.  **Counters** will increment to a specific number then 'roll-over' 
back to zero.  **Gauges** are an instantaneous feedback of a value.

Enrichments/Dimensions are strings applied to provide context to a metric.  For example the CPU name and/or number along 
with the utilization metric.

`metrics_group_type` is a string, but we make specific recommendations in this document to maintain consistency.

## CPU

The CPU utilization should be for each CPU independently and is defined as mean CPU utilization over the polling 
interval, reported as percentage with two significant decimal places.  The reason for this is to avoid the 'noisy' 
nature of instantaneous values.

- Some devices report both the __average__ and __instantaneous__ utilization. In these cases, the value for the 
__average__ over the polling interval should be reported.

- Some devices report a load average in addition to CPU utilization. This value should **not** be reported.

- Some devices mislabel load average as CPU utilization and vice-versa. The value to be reported should be done 
after careful inspection and determination of which is the actual CPU utilization.

- The CPU should be classified as `control` or `data`.

- CPUs should either report a `cpu_name` or `cpu_no` or both.

#### Structure
* metrics_group_type: cpu
* Metrics: 
    - cpu_utilization:\<GAUGE\>
* Dimensions
    - cpu_type:\<string\> (control, data, etc.)
    - cpu_no:\<string\> and/or
    - cpu_name:\<string\>
              
#### Example
```
cpu_utilization:56.2
cpu_type:control
cpu_no:5.1
cpu_name:packet
```

## Memory

Memory is always normalized to bytes.  'Total' memory should always be the amount of physical memory, and this should 
be split where necessary into DRAM or Heap.

- Total (physical memory) and utilized memory in bytes. Downstream systems would calculate percentage utilization.

- Total (physical memory)/utilized would be reported for each type of memory - e.g. dram or heap.

- If the memory metrics are reported by the device in kilobytes and megabytes (or some other unit), they should be 
normalized to bytes.

#### Structure
* metrics_group_type: memory
* Metrics: 
    - memory_total:\<GAUGE\>
    - memory_used:\<GAUGE\>
* Dimensions
    - memory_type:\<string\> (heap, dram, etc.)
              
#### Example
```
memory_total:4294967296
memory_used:1932735283
memory_type:dram
```

## Environment

Environment is currently limited to Power Supplies and Fans.

- Total number of fans and working fans.

- Total number of power supplies and working power supplies.  Note that some devices will return a power supply as 
'working' without it being energized.

#### Structure
* metrics_group_type: environment
* Metrics: 
    - fans_total:\<GAUGE\>
    - fans_ok:\<GAUGE\>
    - power_units_total:\<GAUGE\>
    - power_units_on:\<GAUGE\>
              
#### Example
```
fans_total:4
fans_ok:4
power_units_total:2
power_units_on:1
```

## Temperature

Temperature should be normalized to Fahrenheit for each sensor available.

#### Structure
* metrics_group_type: temperature
* Metrics: 
    - temperature_fahrenheit:\<GAUGE\>
* Dimensions:
    - sensor:\<string\>
              
#### Example
```
temperature_fahrenheit:100.5
sensor:cpu_5_1
```

## Status

The possible values for `polling_status` are enumerated in [DEVICE_METRICS_STATES].  Effectively an integer is returned 
according to the states.

- (0) Success
- (1) Authentication Failure
- (2) Network Failure
- (3) Timeout
- (4) Partial Metric Failure
- (5) Internal Failure
- (6) Missing Metrics
- (7) Ping Failure

#### Structure
* metrics_group_type: status
* Metrics: 
    - polling_status:\<GAUGE\>
  
#### Example
```
polling_status:0
```

A more complete exploration follows in [Device Polling Status](#device-polling-status) & 
[Plugin Execution Flow](#plugin-execution-flow).

## Device Polling Status

The possible values for `polling_status` are enumerated in [DEVICE_METRICS_STATES].  Effectively an integer is returned 
according to the states.

#### (0) Success
All metrics for the device have been collected and the collection cycle is complete.

#### (1) Authentication Failure
One or more metrics failed to be collected due to an API or SNMP authentication failure.

#### (2) Network Failure
One or more metrics failed to be collected due to an API or SNMP connection failure.

#### (3) Timeout
The attempt to connect to the device timed out.  Checking ACLs and connection to the device will usually help here. This
is also raised under snmp v2c when the community strings are incorrect.

#### (4) Partial Metric Failure
At least one metric collection attempt was successful, but at least one other attempt failed.

#### (5) Internal Failure
Metric collection failed due to a problem with the collection code.  These are usually unusual exception states that
indicate a problem with the plugin code.

#### (6) Missing Metrics
The metric collection code executed without error, but the results were empty.

#### (7) Ping Failure
Attempting to ping the device yielded either a [PanoptesPingException], or resulted in total packet loss (i.e. a packet 
loss pct of 100%).  Note that this requires a [PanoptesPollingStatus] `ping` set to True.

## Plugin Execution Flow

The flow of the different states can be difficult to visualize - this documents basic flow;

![Device Polling Flowchart][DevicePollingFlow]

(End states are indicated in light blue.)

At any point during the plugin execution an __Internal Failure(5)__ can arise, and is therefore not shown on this flow.

At the start of execution, a connection attempt is made.  Here we differentiate between a __Timeout(3)__ (failure to 
respond within an amount of time) and a __Network Failure(2)__ (url error, refused connection or a deterministic failure
 state).

If the `ping` argument to [PanoptesPollingStatus] is set to True and a __Timeout(3)__ or __Network Failure(2)__ 
occurred, we ping the endpoint.  If this succeeds, the initial failure state of __Timeout(3)__ or __Network Failure(2)__
is preserved.  If the ping fails, then __Ping Failure(7)__ is returned.

if the first metric and all subsequent metrics are returned, a __Success(0)__ is recorded.  If the first metric is 
collected, and all subsequent metrics fail, then __Partial Metric Failure(4)__ is returned.  If all metric collections 
fail, __Missing Metrics(6)__ is returned.


[DEVICE_METRICS_STATES]: ../yahoo_panoptes/plugins/polling/utilities/polling_status.py
[PanoptesPingException]: ../yahoo_panoptes/framework/utilities/ping.py
[PanoptesPollingStatus]: ../yahoo_panoptes/plugins/polling/utilities/polling_status.py

[DevicePollingFlow]: ../docs/device_polling_flow.svg "Device Polling Flowchart"
