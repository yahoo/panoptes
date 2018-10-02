#!/usr/bin/env python
import sys
import os
from setuptools import setup, find_packages

version = '1.1'

if ('CI' in os.environ) or ('CONTINUOUS_INTEGRATION' in os.environ):
    if 'SCREWDRIVER' in os.environ:
        build_number = os.environ['BUILD_NUMBER']
    elif 'TRAVIS' in os.environ:
        build_number = os.environ['TRAVIS_BUILD_NUMBER']
    else:
        sys.exit('We currently only support building CI builds with Screwdriver or Travis')

    IN_CI_PIPELINE = True
    version = '.'.join([version, build_number])
else:
    version += '.0'

# Read the long description from README.md
with open('README.md') as f:
    long_description = f.read()


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
    'version': version,
    'long_description': long_description,
    'long_description_content_type': "text/markdown",
    'packages': find_packages(),
    'package_data': {
        'yahoo_panoptes.framework': [
            'panoptes_configspec.ini',
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
