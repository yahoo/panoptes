"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module provides convenience classes to interact with Celery: there are classes for representing Celery
Configuration, Instances and an in-memory Scheduler
"""

from builtins import object
import copy
import datetime
import heapq
import pytz
import random
import six
import threading
import time
import math

from celery import Celery
from celery.beat import Scheduler, event_t, ScheduleEntry
from celery.schedules import schedstate, maybe_schedule

from yahoo_panoptes.framework import const
from yahoo_panoptes.framework.context import PanoptesContext
from yahoo_panoptes.framework.exceptions import PanoptesBaseException
from yahoo_panoptes.framework.validators import PanoptesValidators
from yahoo_panoptes.framework.configuration_manager import PanoptesRedisConnectionConfiguration

thread_lock = threading.Lock()


class PanoptesCeleryError(PanoptesBaseException):
    """
    The exception class for Panoptes Celery errors
    """
    pass


class PanoptesCeleryConfig(object):
    """
    The base Celery Config class for Panoptes

    This class would only contain the attributes used to configure a Celery app
    """
    celery_accept_content = [u'application/json', u'json']
    worker_prefetch_multiplier = 1
    task_acks_late = True

    def __init__(self, app_name):
        assert PanoptesValidators.valid_nonempty_string(app_name), u'app_name must be a non-empty string'
        self._celery_app_name = app_name

    @property
    def app_name(self):
        """
        The Celery App name

        Returns:
            str: The Celery App name
        """
        return self._celery_app_name


class PanoptesCeleryValidators(object):
    @classmethod
    def valid_celery_config(cls, celery_config):
        """
        valid_celery_config(cls, celery_config)

        Checks if the passed object is an instance of PanoptesCeleryConfig

        Args:
            celery_config (PanoptesCeleryConfig): The object to check

        Returns:
            bool: True if the object is not null and is an instance of PanoptesCeleryConfig
        """
        return celery_config and isinstance(celery_config, PanoptesCeleryConfig)


class PanoptesCeleryInstance(object):
    """
    Create a Celery instance

    Args:
        panoptes_context (PanoptesContext): The PanoptesContext to use. The system wide configuration associated with \
        the context would be used to create the Celery application - specially, the celery_broker would be set to the \
        redis_client from the configuration attached to the context
        celery_config (PanoptesCeleryConfig): The class containing attributes for the Celery app

    Returns:
        None
    """

    def __init__(self, panoptes_context, celery_config):
        assert isinstance(panoptes_context, PanoptesContext), u'panoptes_context must be an instance of PanoptesContext'
        assert isinstance(celery_config,
                          PanoptesCeleryConfig), u'celery_config must be an instance of PanoptesCeleryConfig'

        logger = panoptes_context.logger

        # TODO: The '0' after celery_broker below refers to the first shard
        celery_broker = panoptes_context.config_object.redis_urls_by_group[const.CELERY_REDIS_GROUP_NAME][0]

        logger.info(u'Creating Celery Application "%s" with broker "%s"' % (
            celery_config.app_name, celery_broker))

        if isinstance(celery_broker, PanoptesRedisConnectionConfiguration):
            celery_broker_url = celery_broker.url
        else:
            celery_broker_url = ';'.join(
                [
                    u'sentinel://{}{}{}'.format(
                        u':{}@'.format(sentinel.password) if sentinel.password else u'',
                        sentinel.host, u':' + str(sentinel.port)
                    ) for sentinel in celery_broker.sentinels]
            )
            celery_config.broker_transport_options = {u'master_name': celery_broker.master_name}

        try:
            self.__celery_instance = Celery(celery_config.app_name, broker=celery_broker_url)
        except Exception as e:
            logger.error(u'Failed to create Celery Application: %s' % repr(e))

        self.__celery_instance.config_from_object(celery_config)
        logger.info(u'Created Celery Application: %s' % self.__celery_instance)

    @property
    def celery(self):
        """
        Returns the instance of the Celery app

        Returns:
            Celery
        """
        return self.__celery_instance


class PanoptesCeleryPluginScheduler(Scheduler):
    """
    The base plugin scheduler class in Panoptes
    """

    def update(self, logger, new_schedule):
        """
        Updates the currently installed scheduled

        Args:
            logger (logging.logger): The logger to use
            new_schedule (dict): The new schedule
        Returns:
            None
        """
        logger.debug(u'New schedule: %s' % str(new_schedule))
        logger.info(u'Going to schedule %d tasks' % len(new_schedule))
        with thread_lock:
            self.merge_inplace(new_schedule)
        logger.info(u'Scheduler now has %d tasks' % len(self.schedule))

    def tick(self, event_t=event_t, min=min, heappop=heapq.heappop, heappush=heapq.heappush):
        """
        Make the tick function thread safe
        """
        with thread_lock:
            response = super(PanoptesCeleryPluginScheduler, self).tick(
                event_t=event_t,
                min=min,
                heappop=heapq.heappop,
                heappush=heapq.heappush
            )

        return response


class PanoptesScheduleEntry(ScheduleEntry):
    """An entry in the scheduler for Panoptes' uniform scheduling.

    Arguments:
        name (str): see :attr:`name`.
        schedule (~celery.schedules.schedule): see :attr:`schedule`.
        args (Tuple): see :attr:`args`.
        kwargs (Dict): see :attr:`kwargs`.
        options (Dict): see :attr:`options`.
        last_run_at (~datetime.datetime): see :attr:`last_run_at`.
        total_run_count (int): see :attr:`total_run_count`.
        relative (bool): Is the time relative to when the server starts?
        last_uniformly_scheduled_at (~datetime.datetime): see :attr:`last_uniformly_scheduled_at`.
    """

    #: The time and date of when this task was last scheduled.
    last_uniformly_scheduled_at = None
    metadata_kv_store = None
    run_at = None
    UNIFORM_PLUGIN_SCHEDULER_BUCKETS = 3607
    UNIFORM_PLUGIN_LAST_UNIFORMLY_SCHEDULED_KEY = 'last_uniformly_scheduled'

    def __init__(self, name=None, task=None, last_run_at=None,
                 total_run_count=None, schedule=None, args=(), kwargs=None,
                 options=None, relative=False, app=None,
                 last_uniformly_scheduled_at=None, metadata_kv_store=None, run_at=None):
        super(PanoptesScheduleEntry, self).__init__(
            name=name, last_run_at=last_run_at, total_run_count=total_run_count, schedule=schedule, args=args,
            kwargs=kwargs, options=options, relative=relative, app=app)
        self.app = app
        self.name = name
        self.task = task
        self.args = args
        self.kwargs = kwargs if kwargs else {}
        self.options = options if options else {}
        self.schedule = maybe_schedule(schedule, relative, app=self.app)
        self.last_run_at = last_run_at
        self.total_run_count = total_run_count or 0
        self.metadata_kv_store = metadata_kv_store
        self.run_at = run_at
        print('Populating last_uniformly_scheduled_at: %s for %s, run_count: %d' % (last_uniformly_scheduled_at,
                                                                                    self.name,
                                                                                    self.total_run_count))

        self.last_uniformly_scheduled_at = last_uniformly_scheduled_at or time.time()

        run_after_uniformily_scheduled = False
        if self.last_run_at:
            run_after_uniformily_scheduled = \
                (float(self.last_run_at.strftime('%s')) - float(self.last_uniformly_scheduled_at)) > 0

        print('%s last run at %s, run after uniformily scheduled: %s' % (
        self.name, self.last_run_at, run_after_uniformily_scheduled))

        if self.last_uniformly_scheduled_at is None or not run_after_uniformily_scheduled:
            print('%s was never uniformily scheduled or has not run after uniform scheduling' % self.name)
            try:
                splay_s = self.schedule.seconds * float(
                    (hash(self.name) % self.UNIFORM_PLUGIN_SCHEDULER_BUCKETS)) / self.UNIFORM_PLUGIN_SCHEDULER_BUCKETS
                value = super(PanoptesScheduleEntry, self).is_due()
                if not value.is_due:
                    splay_s += value.next
                self.run_at = time.time() + splay_s
                print('Splaying %s by %f seconds' % (self.name, splay_s))
            except Exception as e:
                print('Exception for entry %s: %s' % (self.name, repr(e)))
        else:
            print('%s was last uniformily scheduled at %s' % (self.name, str(self.last_uniformly_scheduled_at)))

    def _next_instance(self, last_run_at=None):
        """Return new instance, with date and count fields updated."""
        return self.__class__(**dict(
            self,
            last_run_at=last_run_at or self.default_now(),
            last_uniformly_scheduled_at=self.last_uniformly_scheduled_at or self.default_now(),
            metadata_kv_store=self.metadata_kv_store,
            run_at=self.run_at,
            total_run_count=self.total_run_count + 1,
        ))

    __next__ = next = _next_instance  # for 2to3

    def is_due(self):
        try:
            if self.run_at and self.total_run_count < 1:
                run_in = self.run_at - time.time()
                if run_in > 0:
                    print('Splay for %s' % self.name)
                    value = schedstate(is_due=False, next=run_in)
                else:
                    print('Splay - run now for %s' % self.name)
                    value = schedstate(is_due=True, next=self.schedule.seconds)
            elif not self.last_run_at:
                print('No splay, but no last_run_at %s' % self.name)
                value = schedstate(is_due=True, next=self.schedule.seconds)
            else:
                print('Super for %s, last_run_at %s' % (self.name, str(self.last_run_at)))
                value = super(PanoptesScheduleEntry, self).is_due()

            print('Scheduling %s: run_count %d, schedstate %s' % (self.name, self.total_run_count, str(value)))

            if self.metadata_kv_store:
                try:
                    last_uniformly_scheduled_at_key = ':'.join([
                        'plugin_metadata',
                        self.name,
                        self.UNIFORM_PLUGIN_LAST_UNIFORMLY_SCHEDULED_KEY]
                    )

                    print('Setting key %s to %s' % (
                    last_uniformly_scheduled_at_key, str(self.last_uniformly_scheduled_at)))

                    self.metadata_kv_store.set(
                        last_uniformly_scheduled_at_key,
                        str(self.last_uniformly_scheduled_at),
                        expire=int(self.schedule.seconds * 2))
                except Exception as e:
                    print('Error while setting: %s' % repr(e))
        except AttributeError:
            return schedstate(is_due=False, next=60)

        return value


class PanoptesCeleryPluginUniformScheduler(PanoptesCeleryPluginScheduler):
    """
    Scheduler for uniformly distributing tasks in Panoptes
    """

    Entry = PanoptesScheduleEntry
    UNIFORM_PLUGIN_LAST_UNIFORMLY_SCHEDULED_KEY = 'last_uniformly_scheduled'

    def merge_inplace(self, b):
        metadata_kv_store = self.panoptes_context.get_kv_store(self.metadata_kv_store_class)
        schedule = self.schedule
        A, B = set(schedule), set(b)

        # Remove items from disk not in the schedule anymore.
        for key in A ^ B:
            schedule.pop(key, None)

        # Update and add new items in the schedule
        for key in B:
            try:
                last_uniformly_scheduled_at_key = ':'.join([
                    'plugin_metadata',
                    key,
                    self.UNIFORM_PLUGIN_LAST_UNIFORMLY_SCHEDULED_KEY]
                )

                last_uniformly_scheduled_at = metadata_kv_store.get(
                    last_uniformly_scheduled_at_key)

                print('Got %s using key %s' % (str(last_uniformly_scheduled_at), last_uniformly_scheduled_at_key))
                b[key]['last_uniformly_scheduled_at'] = last_uniformly_scheduled_at
                b[key]['metadata_kv_store'] = metadata_kv_store
            except Exception as e:
                print('Error: %s' % repr(e))

            entry = self.Entry(**dict(b[key], name=key, app=self.app))
            if schedule.get(key):
                schedule[key].update(entry)
            else:
                schedule[key] = entry

    # pylint disable=redefined-outer-name
    def tick(self, event_t=event_t, min=min, heappop=heapq.heappop,
             heappush=heapq.heappush):
        """Run a tick - one iteration of the scheduler.

        Executes one due task per call.

        Returns:
            float: preferred delay in seconds for next call.
        """
        adjust = self.adjust
        max_interval = self.max_interval
        if (self._heap is None or
                not self.schedules_equal(self.old_schedulers, self.schedule)):
            self.old_schedulers = copy.copy(self.schedule)
            print('Repopulating heap')
            self.populate_heap()
            # print('Heap: %s' % str(self._heap))

        H = self._heap

        if not H:
            return max_interval

        event = H[0]
        entry = event[2]
        is_due, next_time_to_run = self.is_due(entry)
        if is_due:
            verify = heappop(H)
            if verify is event:
                next_entry = self.reserve(entry)
                self.apply_entry(entry, producer=self.producer)
                print('Setting next time to run for %s: %f' % (
                entry.name, self._when(next_entry, next_time_to_run) - time.time()))
                heappush(H, event_t(self._when(next_entry, next_time_to_run),
                                    event[1], next_entry))
                return 0
            else:
                print('verify != event')
                heappush(H, verify)
                return min(verify[0], max_interval)
        return min(adjust(next_time_to_run) or max_interval, max_interval)
