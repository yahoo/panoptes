"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
from __future__ import absolute_import
import unittest
import signal

from celery import app
from mock import create_autospec, patch, MagicMock, Mock

from celery.beat import Service

from yahoo_panoptes.framework.celery_manager import PanoptesCeleryConfig, PanoptesCeleryPluginScheduler
from yahoo_panoptes.framework.resources import PanoptesContext
from yahoo_panoptes.framework.plugins.scheduler import PanoptesPluginScheduler
from yahoo_panoptes.framework.utilities.tour_of_duty import PanoptesTourOfDuty

from .test_framework import PanoptesTestKeyValueStore, panoptes_mock_kazoo_client, panoptes_mock_redis_strict_client
from .helpers import get_test_conf_file


def _callback(*args):
    pass


def _callback_exception():
    raise Exception


def _callback_no_args():
    pass


def _mock_is_set_true():
    return True


class TestPanoptesPluginScheduler(unittest.TestCase):
    @patch(u'redis.StrictRedis', panoptes_mock_redis_strict_client)
    @patch(u'kazoo.client.KazooClient', panoptes_mock_kazoo_client)
    def setUp(self):
        self.my_dir, self.panoptes_test_conf_file = get_test_conf_file()
        self._panoptes_context = PanoptesContext(self.panoptes_test_conf_file,
                                                 key_value_store_class_list=[PanoptesTestKeyValueStore],
                                                 create_message_producer=False, async_message_producer=False,
                                                 create_zookeeper_client=True)
        self._celery_config = PanoptesCeleryConfig(app_name=u"Polling Plugin Test")
        self._scheduler = PanoptesPluginScheduler(
            panoptes_context=self._panoptes_context,
            plugin_type=u"polling",
            plugin_type_display_name=u"Polling",
            celery_config=self._celery_config,
            lock_timeout=1,
            plugin_scheduler_task=_callback
        )

    def test_start_basic_operations(self):
        # Test bad input
        with self.assertRaises(AssertionError):
            PanoptesPluginScheduler(u"test", u"polling", u"Polling", self._celery_config, 1,
                                    _callback)
        with self.assertRaises(AssertionError):
            PanoptesPluginScheduler(self._panoptes_context, u"", u"Polling", self._celery_config, 1,
                                    _callback)
        with self.assertRaises(AssertionError):
            PanoptesPluginScheduler(self._panoptes_context, u"polling", u"", self._celery_config, 1,
                                    _callback)
        with self.assertRaises(AssertionError):
            PanoptesPluginScheduler(self._panoptes_context, u"polling", u"Polling", u"Test", 1,
                                    _callback)
        with self.assertRaises(AssertionError):
            PanoptesPluginScheduler(self._panoptes_context, u"polling", u"Polling", self._celery_config, 0,
                                    _callback)
        with self.assertRaises(AssertionError):
            PanoptesPluginScheduler(self._panoptes_context, u"polling", u"Polling", u"Test", 1,
                                    object)

        # Test locking error when starting up the scheduler
        mock_lock = MagicMock(side_effect=Exception)
        with patch(u'yahoo_panoptes.framework.plugins.scheduler.PanoptesLock', mock_lock):
            with self.assertRaises(SystemExit):
                self._scheduler.start()

        celery_app = self._scheduler.start()
        self.assertIsInstance(celery_app, app.base.Celery)

    def test_redundant_shutdown_signal(self):
        celery_app = self._scheduler.start()
        celery_beat_service = Service(celery_app, max_interval=None, schedule_filename=None,
                                      scheduler_cls=PanoptesCeleryPluginScheduler)
        self._scheduler.run(celery_beat_service)

        temp_is_set = self._scheduler._shutdown_plugin_scheduler.is_set

        self._scheduler._shutdown_plugin_scheduler.is_set = _mock_is_set_true
        self._scheduler._signal_handler(signal.SIGTERM, None)  # pragma: no cover
        self._scheduler._shutdown()
        self.assertTrue(self._scheduler._t.isAlive())

        with self.assertRaises(SystemExit):
            self._scheduler._shutdown_plugin_scheduler.is_set = temp_is_set
            self._scheduler._signal_handler(signal.SIGTERM, None)  # pragma: no cover

    def test_shutdown_after_tour_of_duty(self):
        mock_tour_of_duty = create_autospec(PanoptesTourOfDuty)
        mock_tour_of_duty.completed.return_value = True
        mock_tour_of_duty.tasks_completed.return_value = True
        mock_tour_of_duty.time_completed.return_value = True
        mock_tour_of_duty.memory_growth_completed.return_value = True

        with patch(u'yahoo_panoptes.framework.plugins.scheduler.PanoptesTourOfDuty', mock_tour_of_duty):
            self._scheduler = PanoptesPluginScheduler(
                panoptes_context=self._panoptes_context,
                plugin_type=u"polling",
                plugin_type_display_name=u"Polling",
                celery_config=self._celery_config,
                lock_timeout=1,
                plugin_scheduler_task=_callback
            )
            celery_app = self._scheduler.start()
            celery_beat_service = Service(celery_app, max_interval=None, schedule_filename=None,
                                          scheduler_cls=PanoptesCeleryPluginScheduler)
            self._scheduler.run(celery_beat_service)

    def test_celery_beat_error(self):
        mock_celery_instance = MagicMock(side_effect=Exception)
        with patch(u'yahoo_panoptes.framework.plugins.scheduler.PanoptesCeleryInstance', mock_celery_instance):
            celery_app = self._scheduler.start()
            self.assertIsNone(celery_app)
