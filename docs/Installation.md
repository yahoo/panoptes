## Table of Contents

- [Pre-requisites](#pre-requisites)
- [Installation](#installation)
- [Configuration](#configuration)
- [Usage](#usage)

### Pre-requisites

#### System Requirements

If you would be installing the entire stack on a single host, we recommend using a host with at least 8 Cores, 16GB RAM and 50GB Disk Space.

#### OS

Panoptes has been extensively tested on Redhat Linux, though it should run on any distribution that's compatible with LSB. 

The OS should also have the following packages installed:

 - gcc
 - gcc-c++
 - python-dev
 - openssl-devel

#### Python

Panoptes currently supports Python 2.7 only. You can download the latest stable version of Python 2.7 from [here](https://www.python.org/downloads/release/python-2715/)

python-virtualenv should also be installed in order to run panoptes in a contained python environment, as per the examples below.

#### Dependencies

Before downloading and installing Panoptes, you would need the following services installed and configured

- [Redis](#Redis)
- [Zookeeper](#Zookeeper)
- [Kafka](#Kafka)
- [InfluxDB](#InfluxDB)
- [Grafana](#Grafana)

###### Redis

Panoptes has been tested with Redis 3.0.7, which can be downloaded from [here](https://redis.io/download#other-versions)

###### Zookeeper

Panoptes has been tested with Zookeeper version 3.4.5 - download it from [here](https://archive.apache.org/dist/zookeeper/zookeeper-3.4.5/)

###### Kafka

Panoptes has been tested with Kafka version 0.10.2.0 - get it [here](https://kafka.apache.org/downloads#0.10.2.1)

###### InfluxDB

Please follow [these instructions](https://portal.influxdata.com/downloads) to download and install InfluxDB

###### Grafana

You can get Grafana from [here](https://grafana.com/get) 

### Installation

Install Panoptes by running the following commands:

```
sudo useradd panoptes
sudo -su panoptes
cd ~
mkdir -p /home/panoptes/conf
mkdir -p /home/panoptes/log
virtualenv -p python2.7 package
source ~/package/bin/activate
pip install --upgrade setuptools
pip install yahoo_panoptes
```

### Configuration
Panoptes is configured with ini style configuration files

`/home/panoptes/conf/panoptes.ini` is the main configuration file; and you can find an examples of config files under `examples`

For a quick start, you can copy all config files under `examples` to `/home/panoptes`

### Usage

After adjusting the config files to your environment, start the following services. Note that these services run in the foreground and should be run under a job control system like supervisord or daemontools for production usage.

If using the example configuration files perform the following commands first before starting each of the services:

```bash
mkdir -p /home/panoptes/plugins/discovery
mkdir -p /home/panoptes/plugins/polling
mkdir -p /home/panoptes/plugins/enrichment

mkdir -p /home/panoptes/log/discovery/agent
mkdir -p /home/panoptes/log/polling/scheduler/
mkdir -p /home/panoptes/log/polling/agent/
mkdir -p /home/panoptes/log/resources/
mkdir -p /home/panoptes/log/enrichment/scheduler/
mkdir -p /home/panoptes/log/enrichment/agent/
mkdir -p /home/panoptes/log/consumers/influxdb/
```



The services should be started in the order list below.

### Discovery Plugin Scheduler
```bash
mkdir -p /home/panoptes/log/discovery/scheduler
celery beat -A yahoo_panoptes.discovery.discovery_plugin_scheduler -l info -S yahoo_panoptes.framework.celery_manager.PanoptesCeleryPluginScheduler
```

### Discovery Plugin Agent
```bash
mkdir -p /home/panoptes/log/discovery/agent
celery worker -A yahoo_panoptes.discovery.discovery_plugin_agent -l info -f /home/panoptes/log/discovery/agent/discovery_plugin_agent_celery_worker.log -Q discovery_plugin_agent -n discovery_plugin_agent.%h
```
    
### Resource Manager
```bash
mkdir -p /home/panoptes/log/resources/
cd ~
./package/bin/panoptes_resource_manager
```

### Enrichment Plugin Scheduler
```bash
mkdir -p /home/panoptes/log/enrichment/scheduler
celery beat -A yahoo_panoptes.enrichment.enrichment_plugin_scheduler -l info -S yahoo_panoptes.framework.celery_manager.PanoptesCeleryPluginScheduler --pidfile eps.pid
```

### Enrichment Plugin Agent

```bash
echo 'SET panoptes:secrets:snmp_community_string:<site> <snmp community string>' | redis-cli
mkdir -p /home/panoptes/log/enrichment/agent
celery worker -A yahoo_panoptes.enrichment.enrichment_plugin_agent -l info -f /home/panoptes/log/enrichment/agent/enrichment_plugin_agent_celery_worker.log -Q enrichment_plugin_agent -n enrichment_plugin_agent.%h
```

### Polling Plugin Scheduler

```bash
mkdir -p /home/panoptes/log/polling/scheduler
celery beat -A yahoo_panoptes.polling.polling_plugin_scheduler -l info -S yahoo_panoptes.framework.celery_manager.PanoptesCeleryPluginScheduler --pidfile pps.pid
```

### Polling Plugin Agent
```bash
mkdir -p /home/panoptes/log/polling/agent
celery worker -A yahoo_panoptes.polling.polling_plugin_agent -l info -f /home/panoptes/log/polling/agent/polling_plugin_agent_celery_worker_001.log -Q polling_plugin_agent -n polling_plugin_agent_001.%h -Ofair --max-tasks-per-child 10
```

### InfluxDB Consumer
```bash
mkdir -p /home/panoptes/log/consumers/influxdb
cd ~
./package/bin/panoptes_influxdb_consumer
```
