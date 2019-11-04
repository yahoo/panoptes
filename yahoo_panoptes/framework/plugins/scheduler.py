"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module implements classes that can be used by any Plugin Scheduler to setup a Celery App and Celery Beat
"""
from __future__ import print_function
from builtins import object
from builtins import str
import signal
import sys
import threading

from yahoo_panoptes.framework import const
from yahoo_panoptes.framework.validators import PanoptesValidators
from yahoo_panoptes.framework.context import PanoptesContextValidators
from yahoo_panoptes.framework.celery_manager import PanoptesCeleryInstance, PanoptesCeleryValidators
from yahoo_panoptes.framework.utilities.helpers import get_client_id, get_os_tid
from yahoo_panoptes.framework.utilities.lock import PanoptesLock
from yahoo_panoptes.framework.utilities.tour_of_duty import PanoptesTourOfDuty


class PanoptesPluginScheduler(object):
    """
    This class implements methods to start and manage Celery App and Celery Beat for Plugin Schedulers

    Args:
        panoptes_context(PanoptesContext): The Panoptes Context instance that should be used by the Plugin Scheduler
        plugin_type (str): The type of the plugins the Plugin Scheduler would handle
        plugin_type_display_name (str): The display name that should be used by the Plugin Scheduler in logs and errors
        celery_config (PanoptesCeleryConfig): The Celery Config instance that should be used by the Plugin Scheduler
        lock_timeout (int): The number of seconds to wait before a lock times out and is retried
        plugin_scheduler_task (callable): The callback function that the Plugin Scheduler should call every interval

    Returns:
        None
    """

    def __init__(self, panoptes_context, plugin_type, plugin_type_display_name, celery_config, lock_timeout,
                 plugin_scheduler_task, plugin_subtype=None):
        assert PanoptesContextValidators.valid_panoptes_context(
                panoptes_context), u'panoptes_context must be an instance of PanoptesContext'
        assert PanoptesValidators.valid_nonempty_string(plugin_type), u'plugin_type must be a non-empty str'
        assert PanoptesValidators.valid_nonempty_string(plugin_type_display_name), \
            u'plugin_type_display_name must be a non-empty str'
        assert PanoptesCeleryValidators.valid_celery_config(
                celery_config), u'celery_config must be an instance of PanoptesCeleryConfig'
        assert PanoptesValidators.valid_nonzero_integer(lock_timeout), u'lock_timeout must be an int greater than zero'
        assert PanoptesValidators.valid_callback(plugin_scheduler_task), u'plugin_scheduler_task must be a callable'
        assert plugin_type is None or PanoptesValidators.valid_nonempty_string(plugin_type), u'plugin_type must be a ' \
                                                                                             u'None or a non-empty str'

        self._panoptes_context = panoptes_context
        self._config = self._panoptes_context.config_dict
        self._logger = self._panoptes_context.logger
        self._shutdown_plugin_scheduler = threading.Event()
        self._plugin_scheduler_celery_beat_service = None
        self._celery_config = celery_config
        self._celery = None
        self._t = None
        self._lock = None
        self._plugin_type = plugin_type
        self._plugin_subtype = plugin_subtype
        self._plugin_type_display_name = plugin_type_display_name
        self._lock_timeout = lock_timeout
        self._plugin_scheduler_task = plugin_scheduler_task
        self._tour_of_duty = PanoptesTourOfDuty(splay_percent=50)
        self._cycles_without_lock = 0

    def start(self):
        """
        This function starts the Plugin Scheduler. It installs signal handlers, acquire an distributed lock and then
        return a Celery application instance

        The flow of the startup process is follows:
        start -> _celery_beat_service_started (starts) -> plugin_scheduler_task_thread

        The plugin_scheduler_task_thread runs the plugin_scheduler_task every "['plugin_type']['plugin_scan_interval']"
        seconds, which comes from the system wide configuration file

        The reason for this slightly convoluted startup is that because the plugin_scheduler_task needs the Celery Beat
        Service instance object so that it can update the schedule periodically and this only available after the
        _celery_beat_service_started callback function is called by Celery Beat

        Returns:
            celery.app: The Celery App instance to be used by the scheduler

        """

        logger = self._logger

        logger.info(u'%s Plugin Scheduler main thread: OS PID: %d' % (self._plugin_type_display_name, get_os_tid()))

        logger.info(u'"Tour Of Duty" adjusted values: tasks: %d count, time: %d seconds, memory: %dMB' % (
            self._tour_of_duty.adjusted_tasks,
            self._tour_of_duty.adjusted_seconds,
            self._tour_of_duty.adjusted_memory_growth_mb)
        )

        logger.info(u'Setting up signal handlers')
        self._install_signal_handlers()

        client_id = get_client_id(str(const.PLUGIN_CLIENT_ID_PREFIX))
        lock_path = str(const.LOCK_PATH_DELIMITER.join(
            [_f for _f in [
                const.PLUGIN_SCHEDULER_LOCK_PATH,
                self._plugin_type,
                self._plugin_subtype,
                str('lock')
            ] if _f]
        ))

        logger.info(
            u'Creating lock object for %s Plugin Scheduler under lock path "%s"' % (self._plugin_type, lock_path))
        try:
            self._lock = PanoptesLock(context=self._panoptes_context, path=lock_path, timeout=self._lock_timeout,
                                      retries=0, identifier=client_id)
        except Exception as e:
            sys.exit(u'Failed to create lock object: %s' % repr(e))

        if self._lock.locked:
            logger.info(u'Starting Celery Beat Service')
            try:
                self._celery = PanoptesCeleryInstance(self._panoptes_context,
                                                      self._celery_config).celery
                self._celery.conf.update(
                    CELERYBEAT_MAX_LOOP_INTERVAL=self._config[self._plugin_type][u'celerybeat_max_loop_interval'])
            except:
                logger.exception(u'Error trying to start Celery Beat Service')

        return self._celery

    def run(self, sender=None, args=None, **kwargs):
        """
        This function is called after the Celery Beat Service has finished initialization.
        The function (re)installs the signal handlers, since they are overwritten by the Celery Beat Service.
        It stores the reference to the Celery Beat Service instance and starts the Plugin Scheduler thread

        Args:
            sender (celery.beat.Service): The Celery Beat Service instance
            args: Variable length argument list
            **kwargs: Arbitrary keyword argument

        Returns:
            None
        """
        logger = self._logger
        logger.info(u'Reinstalling signal handlers after Celery Beat Service setup')
        self._install_signal_handlers()
        self._plugin_scheduler_celery_beat_service = sender
        self._t = threading.Thread(target=self._plugin_scheduler_task_thread)
        self._t.start()

    def _plugin_scheduler_task_thread(self):
        """
        This function is the entry point of the Plugin Scheduler thread. It checks if the Plugin Scheduler is shutdown
        mode and if not, then calls the plugin_scheduler_task function every 'plugin_scan_interval'
        seconds

        Returns:
            None

        """
        logger = self._logger
        logger.info(u'%s Plugin Scheduler Task thread: OS PID: %d' % (self._plugin_type_display_name, get_os_tid()))

        while not self._shutdown_plugin_scheduler.is_set():
            if self._lock.locked:
                self._cycles_without_lock = 0
                try:
                    self._plugin_scheduler_task(self._plugin_scheduler_celery_beat_service)
                    self._tour_of_duty.increment_task_count()
                except Exception:
                    logger.exception(u'Error trying to execute plugin scheduler task')
            else:
                self._cycles_without_lock += 1
                if self._cycles_without_lock < const.PLUGIN_SCHEDULER_MAX_CYCLES_WITHOUT_LOCK:
                    logger.warn(u'%s Plugin Scheduler lock not held, skipping scheduling cycle' %
                                self._plugin_type_display_name)
                else:
                    logger.warn(u'%s Plugin Scheduler lock not held for %d cycles, shutting down' %
                                self._plugin_type_display_name, self._cycles_without_lock)
                    self._shutdown()

            if self._tour_of_duty.completed:
                why = []
                why += [u'tasks'] if self._tour_of_duty.tasks_completed else []
                why += [u'time'] if self._tour_of_duty.time_completed else []
                why += [u'memory growth'] if self._tour_of_duty.memory_growth_completed else []

                logger.info(u'%s Plugin Scheduler "Tour Of Duty" completed because of %sgoing to shutdown' %
                            (self._plugin_type_display_name, ', '.join(why)))
                self._shutdown()
            self._shutdown_plugin_scheduler.wait(self._config[self._plugin_type][u'plugin_scan_interval'])

        logger.critical(u'%s Plugin Scheduler Task thread shutdown' % self._plugin_type_display_name)

    def _shutdown(self):
        """
        The main shutdown method, which handles two scenarios
        * The Plugin Scheduler thread is alive: sets an event to shutdown the thread
        * The Plugin Scheduler thread is not alive: this can happen if we have not been able to acquire the lock or
        the if the Plugin Scheduler thread quits unexpectedly. In this case, this handler proceeds to call the function
        to shutdown other services (e.g. Celery Beat Service)

        Returns:
            None
        """
        if self._shutdown_plugin_scheduler.is_set():
            print(u'%s Plugin Scheduler already in the process of shutdown, ignoring redundant call')
            return

        shutdown_interval = int(int(self._config[self._plugin_type][u'plugin_scan_interval']) * 2)
        print(u'Shutdown/restart requested - may take up to %s seconds' % shutdown_interval)

        print(u'Signalling for %s Plugin Scheduler Task Thread to shutdown' % self._plugin_type_display_name)
        self._shutdown_plugin_scheduler.set()

        if self._t != threading.currentThread():
            if (self._t is not None) and (self._t.isAlive()):
                self._t.join()

            if (self._t is None) or (not self._t.isAlive()):
                print(u'%s Plugin Scheduler Task Thread is not active - shutting down other services' %
                      self._plugin_type_display_name)
        else:
            print(u'%s Plugin Scheduler shutdown called from plugin scheduler task thread' %
                  self._plugin_type_display_name)

        if self._plugin_scheduler_celery_beat_service:
            print(u'Shutting down Celery Beat Service')
            self._plugin_scheduler_celery_beat_service.stop()

        if self._lock:
            print(u'Releasing lock')
            self._lock.release()

        print(u'Plugin Scheduler shutdown complete')
        sys.exit()

    def _install_signal_handlers(self):
        """
        Installs signal handlers for SIGTERM, SIGINT and SIGHUP

        Returns:
            None
        """
        signal.signal(signal.SIGTERM, self._signal_handler)
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGHUP, self._signal_handler)

    def _signal_handler(self, signal_number, _):
        """
        Signal handler - wraps the _shutdown method with some checks

        Args:
            signal_number (int): The received signal number
            _ (frame): Current stack frame object

        Returns:
            None
        """
        print(u'Caught %s, shutting down %s Plugin Scheduler' % (
            const.SIGNALS_TO_NAMES_DICT[signal_number], self._plugin_type_display_name))

        # If the Plugin Scheduler is already in the process of shutdown, then do nothing - prevents issues
        # with re-entrancy
        if self._shutdown_plugin_scheduler.is_set():
            print(u'%s Plugin Scheduler already in the process of shutdown, ignoring %s' %
                  (self._plugin_type_display_name, const.SIGNALS_TO_NAMES_DICT[signal_number]))
            return

        self._shutdown()
