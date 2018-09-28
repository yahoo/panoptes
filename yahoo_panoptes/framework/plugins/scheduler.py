"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module implements classes that can be used by any Plugin Scheduler to setup a Celery App and Celery Beat
"""
import signal
import sys
import threading

from .. import const
from ..validators import PanoptesValidators
from ..context import PanoptesContextValidators
from ..celery_manager import PanoptesCeleryInstance, PanoptesCeleryValidators
from ..utilities.helpers import get_client_id, get_os_tid
from ..utilities.lock import PanoptesLock


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
                 plugin_scheduler_task):
        assert PanoptesContextValidators.valid_panoptes_context(
                panoptes_context), 'panoptes_context must be an instance of PanoptesContext'
        assert PanoptesValidators.valid_nonempty_string(plugin_type), 'plugin_type must be a non-empty str'
        assert PanoptesValidators.valid_nonempty_string(plugin_type_display_name), \
            'plugin_type_display_name must be a non-empty str'
        assert PanoptesCeleryValidators.valid_celery_config(
                celery_config), 'celery_config must be an instance of PanoptesCeleryConfig'
        assert PanoptesValidators.valid_nonzero_integer(lock_timeout), 'lock_timeout must be an int greater than zero'
        assert PanoptesValidators.valid_callback(plugin_scheduler_task), 'plugin_scheduler_task must be a callable'

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
        self._plugin_type_display_name = plugin_type_display_name
        self._lock_timeout = lock_timeout
        self._plugin_scheduler_task = plugin_scheduler_task

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

        logger.info('%s Plugin Scheduler main thread: OS PID: %d' % (self._plugin_type_display_name, get_os_tid()))

        logger.info('Setting up signal handlers')
        self._install_signal_handlers()

        client_id = get_client_id(const.PLUGIN_CLIENT_ID_PREFIX)
        lock_path = const.PLUGIN_SCHEDULER_LOCK_PATH + '/' + self._plugin_type + '/lock'

        logger.info(
                'Creating lock object for %s Plugin Scheduler under lock path "%s"' % (self._plugin_type, lock_path))
        try:
            self._lock = PanoptesLock(context=self._panoptes_context, path=lock_path, timeout=self._lock_timeout,
                                      retries=0, identifier=client_id)
        except Exception as e:
            sys.exit('Failed to create lock object: %s' % repr(e))

        if self._lock.locked:
            logger.info('Starting Celery Beat Service')
            try:
                self._celery = PanoptesCeleryInstance(self._panoptes_context,
                                                      self._celery_config).celery
                self._celery.conf.update(
                        CELERYBEAT_MAX_LOOP_INTERVAL=self._config[self._plugin_type]['celerybeat_max_loop_interval'])
            except Exception as e:
                logger.error('Error trying to start Celery Beat Service: %s' % str(e))

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
        logger.info('Reinstalling signal handlers after Celery Beat Service setup')
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
        logger.info('%s Plugin Scheduler Task thread: OS PID: %d' % (self._plugin_type_display_name, get_os_tid()))

        while not self._shutdown_plugin_scheduler.is_set():
            if self._lock.locked:
                try:
                    self._plugin_scheduler_task(self._plugin_scheduler_celery_beat_service)
                except Exception:
                    logger.exception('Error trying to execute plugin scheduler task')
            else:
                logger.warn('%s Plugin Scheduler lock not held, skipping scheduling cycle')
            self._shutdown_plugin_scheduler.wait(self._config[self._plugin_type]['plugin_scan_interval'])

        logger.critical('%s Plugin Scheduler Task thread shutdown' % self._plugin_type_display_name)

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
        The signal handler addresses two scenarios:
        * The Plugin Scheduler thread is alive: sets an event to shutdown the thread
        * The Plugin Scheduler thread is not alive: this can happen if we have not been able to acquire the lock or
        the if the Plugin Scheduler thread quits unexpectedly. In this case, this handler proceeds to call the function
        to shutdown other services (e.g. Celery Beat Service)

        In case a lock has not yet been acquired, then it also cancels the pending lock acquisition request
        In case it receives a SIGHUP, it calls startup_plugin_scheduler. This would re-create the PanoptesContext -
        essentially, re-reading the configuration

        Args:
            signal_number (int): The received signal number
            _ (frame): Current stack frame object

        Returns:
            None
        """
        # If the Plugin Scheduler is already in the process of shutdown, then do nothing - prevents issues
        # with re-entrancy
        if self._shutdown_plugin_scheduler.is_set():
            print('%s Plugin Scheduler already in the process of shutdown, ignoring %s' %
                  (self._plugin_type_display_name, const.SIGNALS_TO_NAMES_DICT[signal_number]))
            return

        print('Caught %s, shutting down %s Plugin Scheduler' % (
            const.SIGNALS_TO_NAMES_DICT[signal_number], self._plugin_type_display_name))

        shutdown_interval = int(int(self._config[self._plugin_type]['plugin_scan_interval']) * 2)
        print('Shutdown/restart may take up to %s seconds' % shutdown_interval)

        print('Signalling for %s Plugin Scheduler Task Thread to shutdown' % self._plugin_type_display_name)
        self._shutdown_plugin_scheduler.set()

        if (self._t is not None) and (self._t.isAlive()):
            self._t.join()

        if (self._t is None) or (not self._t.isAlive()):
            print('%s Plugin Scheduler Task Thread is not active - shutting down other services' %
                  self._plugin_type_display_name)

        if self._plugin_scheduler_celery_beat_service:
            print('Shutting down Celery Beat Service')
            self._plugin_scheduler_celery_beat_service.stop()

        if self._lock:
            print('Releasing lock')
            self._lock.release()

        print('%s Plugin Scheduler shutdown complete')
        sys.exit()
