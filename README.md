# Panoptes
> A Global-Scale, Network Telemetry Ecosystem

## Table of Contents

- [Introduction](#introduction)
- [Install](#install)
- [Configuration](#configuration)
- [Usage](#usage)
- [Contribute](#contribute)
- [License](#license)

## Introduction

Panoptes is a Python based network telemetry ecosystem that implements discovery, enrichment and polling. Key features include:

- A modular design with well defined separation of concerns,
- Plugin architecture that enables the implementation of any telemetry collection, enrichment, or transformation,
- Horizontally scalable: supports clustering to add more capacity, and
- Network telemetry specific constructs like SNMP abstractions, built in counter to gauge conversion.

## Install

### Pre-requisites

#### OS

Panoptes has been extensively tested on Redhat Linux, though it should run on any distribution that's compatible with LSB. 

#### Python

Panoptes currently supports Python 2.7 only. You can download the latest stable version of Python 2.7 from [here](https://www.python.org/downloads/release/python-2715/)

#### Dependencies

Before downloading and installing Panoptes, you would need the following services installed and configured

- [Redis](#redis)
- [Zookeeper](#zookeeper)
- [Kafka](#Kafka)

###### Redis

Panoptes has been tested with Redis 3.0.7, which can be downloaded from [here](https://redis.io/download#other-versions)

###### Zookeeper

Panoptes has been tested with Zookeeper version 3.4.5 - download it from [here](https://archive.apache.org/dist/zookeeper/zookeeper-3.4.5/)

###### Kafka

Panoptes has been tested with Kafka version 0.10.2.0 - get it [here](https://kafka.apache.org/downloads#0.10.2.1)

### Installation

Install by running the following commands:

```
useradd panoptes
sudo -u panoptes
mkdir /home/panoptes/log
git clone https://github.com/yahoo/panoptes
virtualenv -p2.7 panoptes_virt
source ~/panoptes_virt/bin/activate
cd ~/panoptes_virt
pip install -e.[deploy]
```

## Configuration
Panoptes is configured with ini style configuration files

`/home/panoptes/conf/panoptes.conf` is the main configuration file; you can find an example of this at `examples/panoptes.ini`

## Usage

After adjusting the config files to your environment, start the following services. Note that these services run in the foreground and should be run under a job control system like supervisord or daemontools for production usage.

The services should be started in the order list below.

### Discovery Plugin Scheduler
```bash
mkdir -p /home/panoptes/log/discovery/scheduler
cd ~/panoptes
celery beat -A yahoo_panoptes.discovery.discovery_plugin_scheduler -l info -S yahoo_panoptes.framework.celery_manager.PanoptesCeleryPluginScheduler
```

### Discovery Plugin Agent
```bash
mkdir -p /home/panoptes/log/discovery/agent
cd ~/panoptes
celery worker -A yahoo_panoptes.discovery.discovery_plugin_agent -l info -f /home/panoptes/log/discovery/agent/discovery_plugin_agent_celery_worker.log -Q discovery_plugin_agent -n discovery_plugin_agent.%h
```
    
### Resource Manager
```bash
mkdir -p /home/panoptes/log/resources/
cd ~/panoptes
python -m yahoo_panoptes.resources.manager
```

### Enrichment Plugin Scheduler
```bash
mkdir -p /home/panoptes/log/enrichment/scheduler
cd ~/panoptes
celery beat -A yahoo_panoptes.enrichment.enrichment_plugin_scheduler -l info -S yahoo_panoptes.framework.celery_manager.PanoptesCeleryPluginScheduler --pidfile eps.pid
```

### Enrichment Plugin Agent

```bash
mkdir -p /home/panoptes/log/enrichment/agent
cd ~/panoptes
celery worker -A yahoo_panoptes.enrichment.enrichment_plugin_agent -l info -f /home/panoptes/log/enrichment/agent/enrichment_plugin_agent_celery_worker.log -Q enrichment_plugin_agent -n enrichment_plugin_agent.%h
```

### Polling Plugin Scheduler

```bash
mkdir -p /home/panoptes/log/polling/scheduler
cd ~/panoptes
celery beat -A yahoo_panoptes.polling.polling_plugin_scheduler -l info -S yahoo_panoptes.framework.celery_manager.PanoptesCeleryPluginScheduler --pidfile pps.pid
```

### Polling Plugin Agent
```bash
mkdir -p /home/panoptes/log/polling/agent
echo 'SET panoptes:secrets:snmp_community_string:<site> <snmp community string>' | redis-cli
celery worker -A yahoo_panoptes.polling.polling_plugin_agent -l info -f /home/panoptes/log/polling/agent/polling_plugin_agent_celery_worker_001.log -Q polling_plugin_agent -n polling_plugin_agent_001.%h -Ofair --max-tasks-per-child 10
```

## Contribute

We welcome issues, questions, and pull requests.

## Maintainers
* Varun Varma: vvarun@oath.com
* Vivekanand AM: viveka@oath.com
* Ian Holmes: iholmes@oath.com
* James Diss: rexfury@oath.com

## License
This project is licensed under the terms of the [Apache 2.0](LICENSE-Apache-2.0) open source license. Please refer to [LICENSE](LICENSE) for the full terms.
