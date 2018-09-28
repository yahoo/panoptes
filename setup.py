#!/usr/bin/env python
import os
from setuptools import setup, find_packages


def package_scripts():
    """
    Update the "scripts" parameter of the setup_arguments with any scripts
    found in the "scripts" directory.
    :return:
    """
    scripts_list = []
    if os.path.isdir('scripts'):
        scripts_list = [
            os.path.join('scripts', f) for f in os.listdir('scripts')
            ]
    return scripts_list


setup_arguments = {
    'packages': find_packages(),
    'package_data': {
        'yahoo_panoptes.framework': [
            'panoptes_configspec.ini',
        ],
        'yahoo_panoptes.plugins.discovery': [
            '*.panoptes-plugin'
        ],
        'yahoo_panoptes.consumers.influxdb': [
            'influxdb_consumer_configspec.ini',
        ]
    },
    'scripts': package_scripts(),
}
args = setup_arguments


if __name__ == '__main__':
    setup(**setup_arguments)
