[![Slack](https://img.shields.io/badge/slack-panoptescommunity-blue.svg?logo=slack)](https://panoptescommunity.slack.com/open) [![Build Status](https://img.shields.io/travis/yahoo/redislite.svg)](https://travis-ci.org/yahoo/panoptes.svg?branch=master) [![PyPI](https://img.shields.io/pypi/v/yahoo_panoptes.svg)](https://pypi.org/project/yahoo-panoptes) [![Python](https://img.shields.io/badge/python-2.7-blue.svg)](https://pypi.org/project/yahoo-panoptes) [![License](https://img.shields.io/pypi/l/yahoo_panoptes.svg)](https://opensource.org/licenses/Apache-2.0)

# Panoptes
> A Global-Scale Network Telemetry Ecosystem

## Try It!

Try out Panoptes in a [Docker container](https://hub.docker.com/r/panoptes/panoptes_docker). Detailed documentation is [here](https://github.com/yahoo/panoptes_docker).

## Table of Contents

- [Introduction](#introduction)
- [Architecture](#architecture)
- [Concepts](#concepts)
- [Install](#install)
- [Contribute](#contribute)
- [License](#license)

## Introduction

Panoptes is a Python based network telemetry ecosystem that implements discovery, enrichment and polling. Key features include:

- A modular design with well defined separation of concerns,
- Plugin architecture that enables the implementation of any telemetry collection, enrichment, or transformation,
- Horizontally scalable: supports clustering to add more capacity, and
- Network telemetry specific constructs like SNMP abstractions, built in counter to gauge conversion.

## Architecture

![Panoptes Architecture](docs/panoptes_architecture.png?sanitize=true)

## Concepts

Panoptes is built on many primitives like sites, resources, metrics and enrichments which are collected through discovery and polling. [Here is](docs/Concepts.md) a document providing an overview of these concepts.

## Install

If you'd like to try out Panoptes without committing to a full install, checkout out the [Docker container](https://hub.docker.com/r/panoptes/panoptes_docker).

Please follow the instructions [here](https://github.com/yahoo/panoptes/blob/master/docs/Installation.md) to download and install Panoptes.

## Contribute

We welcome issues, questions, and pull requests. Please read the [contributing guidelines](https://github.com/yahoo/panoptes/blob/master/docs/Contributing.md).

## Maintainers
* Varun Varma: vvarun@verizonmedia.com
* Vivekanand AM: viveka@verizonmedia.com
* Ian Holmes: iholmes@verizonmedia.com
* James Diss: rexfury@verizonmedia.com

## License
This project is licensed under the terms of the Apache 2.0 open source license. Please refer to [LICENSE](https://github.com/yahoo/panoptes/blob/master/LICENSE) for the full terms.
