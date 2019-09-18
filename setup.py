#!/usr/bin/env python

from setuptools import setup

setup(
    setup_requires=['pbr'],
    pbr=True,
    package_data={
        'yahoo_panoptes.framework': [
            'panoptes_configspec.ini',
        ],
        'yahoo_panoptes.consumers.influxdb': [
            'influxdb_consumer_configspec.ini',
        ]
    },
)
