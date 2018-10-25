#!/usr/bin/env python

"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""

import os

from mock import patch

from yahoo_panoptes.framework.celery_manager import PanoptesCeleryConfig
from yahoo_panoptes.framework.resources import PanoptesContext
from yahoo_panoptes.framework.plugins.scheduler import PanoptesPluginScheduler

from tests.test_framework import PanoptesTestKeyValueStore, panoptes_mock_kazoo_client, panoptes_mock_redis_strict_client


def mock_wait(*args):
    print "#### waiting..."


def _callback(*args):
    print "#### in _callback"


@patch('redis.StrictRedis', panoptes_mock_redis_strict_client)
@patch('kazoo.client.KazooClient', panoptes_mock_kazoo_client)
def main(*args):
    my_dir = os.path.dirname(os.path.realpath(__file__))
    panoptes_test_conf_file = os.path.join(my_dir, 'config_files/test_panoptes_config.ini')
    panoptes_context = PanoptesContext(panoptes_test_conf_file,
                                       key_value_store_class_list=[PanoptesTestKeyValueStore],
                                       create_message_producer=False, async_message_producer=False,
                                       create_zookeeper_client=True)
    print "##### In Main"
    celery_config = PanoptesCeleryConfig(app_name="Polling Plugin Test")
    scheduler = PanoptesPluginScheduler(panoptes_context, "polling", "Polling", celery_config, 1,
                                        _callback)

    scheduler.start()
    print "#### os.getpid(): %s" % os.getpid()
    scheduler.run()

    # TODO kill plugin_scheduler_task_thread
    # os.kill(os.getpid(), signal.SIGUSR1)

if __name__ == "__main__":
    main()
