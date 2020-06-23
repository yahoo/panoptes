"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module provides convenience classes to interact with Celery: there are classes for representing Celery
Configuration, Instances and an in-memory Scheduler
"""
from builtins import object
import heapq
import time
import copy
import mmh3
import threading

from celery import Celery
from celery.beat import Scheduler, ScheduleEntry, event_t
from celery.schedules import schedstate, crontab

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
        else:  # isinstance(celery_broker, PanoptesRedisSentinelConnectionConfiguration)
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

    def merge_inplace(self, b, called_by_panoptes=False):
        """
        Updates the set of schedule entries.

        This function is defined within the PanoptesCeleryPluginScheduler
        class to allow for backwards compatibility with the new
        called_by_panoptes argument. The argument is needed for
        the UniformScheduler Class
        """
        super(PanoptesCeleryPluginScheduler, self).merge_inplace(b)

    def update(self, logger, new_schedule, called_by_panoptes=False):
        """
        Updates the currently installed scheduled

        Args:
            logger (logging.logger): The logger to use
            new_schedule (dict): The new schedule
            called_by_panoptes (bool): Was .update()
            called by panoptes or a function within celery/beat.py
        Returns:
            None
        """
        logger.debug(u'New schedule: %s' % str(new_schedule))
        logger.info(u'Going to schedule %d tasks' % len(new_schedule))
        with thread_lock:
            self.merge_inplace(new_schedule, called_by_panoptes)
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
    """An entry in the scheduler.

    Arguments:
        name (str): see :attr:`name`.
        schedule (~celery.schedules.schedule): see :attr:`schedule`.
        args (Tuple): see :attr:`args`.
        kwargs (Dict): see :attr:`kwargs`.
        options (Dict): see :attr:`options`.
        last_run_at (~datetime.datetime): see :attr:`last_run_at`.
        total_run_count (int): see :attr:`total_run_count`.
        relative (bool): Is the time relative to when the server starts?
        run_at (int): Epoch time when the schedule entry should run next
        uniformly_scheduled (bool): Whether or not the ScheduleEntry has been
        uniformly scheduled (splay added to the initial due date).
        kv_store: ('redis.client.Redis'): Connection to the KV Store
        last_uniformly_scheduled_at (str): Last time this `job` was scheduled
        by a different scheduler process.
    """

    def __init__(self, name=None, task=None, last_run_at=None,
                 total_run_count=None, schedule=None, args=(),
                 kwargs=None, options=None, relative=None,
                 app=None, run_at=None, uniformly_scheduled=False,
                 kv_store=None, last_uniformly_scheduled_at=None):
        super(PanoptesScheduleEntry, self).__init__(
            name=name, task=task, last_run_at=last_run_at,
            total_run_count=total_run_count, schedule=schedule,
            args=args, kwargs=kwargs, options=options,
            relative=relative, app=app
        )

        self.run_at = run_at
        self.uniformly_scheduled = uniformly_scheduled
        self.kv_store = kv_store
        self.last_uniformly_scheduled_at = last_uniformly_scheduled_at

        if isinstance(self.schedule, crontab):
            return

        try:
            # Only add splay on the first run
            if not self.uniformly_scheduled:
                print(f'{self.name} has not been uniformly scheduled by this process.')

                plugin_execution_frequency = self.schedule.seconds
                time_now = time.time()
                expected_execution_date_from_last_schedule = 0

                print(f'Checking to see if {self.name} has been scheduled in the '
                      f'last {plugin_execution_frequency} seconds')
                if self.last_uniformly_scheduled_at is not None:
                    print(f'{self.name} was last uniformly scheduled at {self.last_uniformly_scheduled_at}')
                    self.last_uniformly_scheduled_at = float(self.last_uniformly_scheduled_at)
                    expected_execution_date_from_last_schedule = self.last_uniformly_scheduled_at + \
                                                                 plugin_execution_frequency
                else:
                    print(f'{self.name} has never been uniformly scheduled')

                if expected_execution_date_from_last_schedule >= time_now > self.last_uniformly_scheduled_at:
                    print(f'Picking up where the previous scheduler process left off. Scheduling '
                          f'{self.name} to execute in '
                          f'{expected_execution_date_from_last_schedule - time.time()} seconds.')
                    self.run_at = expected_execution_date_from_last_schedule
                    return
                else:
                    print(f'Unable to schedule {self.name} where the previous scheduler process left off')

                splay_s = mmh3.hash(self.name, signed=False) % min(self.schedule.seconds, 60)

                self.run_at = time_now + splay_s
                print(f'Uniformly scheduling {self.name} with splay {splay_s} due {self.run_at}')

        except Exception as e:
            print(f'Error Scheduling {repr(e)}')

    @staticmethod
    def schedule_entry_unique_identifier(schedule, args):
        return "{}|{}".format(str(float(schedule.seconds)), "|".join(map(str, args)))

    def __next__(self, last_run_at=None):
        # Set uniformly_scheduled to True so splay isn't added again.
        return self.__class__(**dict(
            self,
            last_run_at=self.default_now(),
            total_run_count=self.total_run_count + 1,
            uniformly_scheduled=True
        ))

    def is_due(self):
        try:
            if not self.uniformly_scheduled and not isinstance(self.schedule, crontab):
                run_in = self.run_at - time.time()

                if run_in > 0:
                    value = schedstate(is_due=False, next=run_in)
                else:
                    value = schedstate(is_due=True, next=self.schedule.seconds)

            else:
                value = super(PanoptesScheduleEntry, self).is_due()

        except Exception as e:
            # If there is an issue with the key set in redis
            # Assume 60 second Interval.
            print(f'{self.name} Attribute Error {repr(e)}')
            self.run_at = time.time() + 60
            value = schedstate(is_due=False, next=60)

        if value[0] and self.kv_store:
            # This `ScheduleEntry` is due to be executed.
            # Update the `last_uniformly_scheduled` key in Redis
            uniformly_scheduled = time.time()

            last_uniformly_scheduled_at_key = ':'.join([
                'plugin_metadata',
                self.name,
                'last_uniformly_scheduled'
            ])

            self.kv_store.set(
                last_uniformly_scheduled_at_key,
                str(uniformly_scheduled),
                expire=int(self.schedule.seconds * 2)
            )

        return value


class PanoptesUniformScheduler(PanoptesCeleryPluginScheduler):
    Entry = PanoptesScheduleEntry
    UNIFORM_PLUGIN_LAST_UNIFORMLY_SCHEDULED_KEY = 'last_uniformly_scheduled'
    SCHEDULE_POPULATED = False

    def obtain_last_uniformly_scheduled_time(self, kv_store, key):

        try:
            last_uniformly_scheduled_at_key = ':'.join([
                'plugin_metadata',
                key,
                'last_uniformly_scheduled'
            ])

            last_uniformly_scheduled_at = kv_store.get(last_uniformly_scheduled_at_key)
            return last_uniformly_scheduled_at

        except Exception as e:
            print(f'PanoptesUniformScheduler::obtain_last_uniformly_scheduled_time. {repr(e)}')
            return None

    def merge_inplace(self, b, called_by_panoptes=False):

        if not called_by_panoptes:
            super(PanoptesUniformScheduler, self).merge_inplace(b)
            return

        metadata_kv_store = self.panoptes_context.get_kv_store(self.metadata_kv_store_class)
        schedule = self.schedule

        A, B = set(schedule), set(b)

        for key in A ^ B:
            schedule.pop(key, None)

        for key in B:

            if schedule.get(key):
                panoptes_schedule_entry = schedule[key]

                existing_panoptes_schedule_entry = PanoptesScheduleEntry.\
                    schedule_entry_unique_identifier(panoptes_schedule_entry.schedule, panoptes_schedule_entry.args)
                new_panoptes_schedule_entry_candidate = PanoptesScheduleEntry.\
                    schedule_entry_unique_identifier(b[key]['schedule'], b[key]['args'])

                if existing_panoptes_schedule_entry == new_panoptes_schedule_entry_candidate:
                    print(f'Skipping {new_panoptes_schedule_entry_candidate}, '
                          f'there is already a matching ScheduleEntry being executed')
                    continue
                else:
                    print(f'Found Existing Schedule Entry which didn\'t match the new one.'
                          f'{existing_panoptes_schedule_entry} !== {new_panoptes_schedule_entry_candidate}'
                          f' replacing now.')
                    b[key]['last_uniformly_scheduled_at'] = self.obtain_last_uniformly_scheduled_time(metadata_kv_store,
                                                                                                      key)
                    b[key]['kv_store'] = metadata_kv_store
                    schedule[key].update(self.Entry(**dict(b[key], name=key, app=self.app)))

            else:
                b[key]['last_uniformly_scheduled_at'] = self.obtain_last_uniformly_scheduled_time(metadata_kv_store,
                                                                                                  key)
                b[key]['kv_store'] = metadata_kv_store
                print(f'Entry is {self.Entry}')
                entry = self.Entry(**dict(b[key], name=key, app=self.app))
                schedule[key] = entry

        if called_by_panoptes and not self.SCHEDULE_POPULATED:
            print('Panoptes merge_inplace call finished, setting schedule_populated to loosen tick loop.')
            self.SCHEDULE_POPULATED = True

    def tick(self, event_t=event_t, min=min, heappop=heapq.heappop, heappush=heapq.heappush):
        """
        Make the tick function thread safe

        Run a tick - one iteration of the scheduler.

        Executes on due task per call

        Returns:
            float: preferred delay in seconds for next call.
        """
        with thread_lock:
            adjust = self.adjust
            max_interval = self.max_interval

            if self._heap is None or not self.schedules_equal(self.old_schedulers, self.schedule):
                self.old_schedulers = copy.copy(self.schedule)

                print('Repopulating the heap')
                self.populate_heap()

            H = self._heap

            if not H:
                return max_interval

            # event_t = namedtuple('event_t', ('time', 'priority', 'entry'))
            event = H[0]
            entry = event[2]

            is_due, next_time_to_run = self.is_due(entry)

            if is_due:
                verify = heappop(H)

                if verify is event:
                    next_entry = self.reserve(entry)
                    self.apply_entry(entry, producer=self.producer)
                    heappush(H, event_t(self._when(next_entry, next_time_to_run), event[1], next_entry))
                    return 0
                else:
                    heappush(H, verify)
                    return min(verify[0], max_interval)

            # Temporarily spin in a tight loop until the
            # @beat_init.connect callback occurs and calls run on the
            # (yahoo_panoptes.framework.plugins.scheduler.)PanoptesPluginScheduler
            # which calls update (-> merge_inplace) on the cached schedule.
            if self.SCHEDULE_POPULATED is False:
                return min(adjust(next_time_to_run), 0.01)

            return min(adjust(next_time_to_run) or max_interval, max_interval)
