"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module provides convenience classes to interact with Celery: there are classes for representing Celery
Configuration, Instances and an in-memory Scheduler
"""
import heapq
import threading

from celery import Celery
from celery.beat import Scheduler, event_t

from . import const
from .context import PanoptesContext
from .exceptions import PanoptesBaseException
from .validators import PanoptesValidators

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
    celery_accept_content = ['application/json', 'json']
    worker_prefetch_multiplier = 1
    task_acks_late = True

    def __init__(self, app_name):
        assert PanoptesValidators.valid_nonempty_string(app_name), 'app_name must be a non-empty string'
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
        assert isinstance(panoptes_context, PanoptesContext), 'panoptes_context must be an instance of PanoptesContext'
        assert isinstance(celery_config,
                          PanoptesCeleryConfig), 'celery_config must be an instance of PanoptesCeleryConfig'
        logger = panoptes_context.logger
        celery_broker = panoptes_context.config_object.redis_urls_by_group[const.DEFAULT_REDIS_GROUP_NAME]
        logger.info('Creating Celery Application "%s" with broker "%s"' % (
            celery_config.app_name, celery_broker))
        try:
            # TODO: The '0' after celery_broker below refers to the first shard
            self.__celery_instance = Celery(celery_config.app_name, broker=celery_broker[0].url)
        except Exception as e:
            logger.error('Failed to create Celery Application: %s' % str(e))

        self.__celery_instance.config_from_object(celery_config)
        logger.info('Created Celery Application: %s' % self.__celery_instance)

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
        logger.debug('New schedule: %s' % str(new_schedule))
        logger.info('Going to schedule %d tasks' % len(new_schedule))
        with thread_lock:
            self.merge_inplace(new_schedule)
            logger.info('Going to add default entries')
            self.install_default_entries(new_schedule)
        logger.info('Scheduler now has %d tasks' % len(self.schedule))

    def tick(self, event_t=event_t, min=min, heappop=heapq.heappop, heappush=heapq.heappush):
        """
        Make the tick function thread safe
        """
        with thread_lock:
            super(PanoptesCeleryPluginScheduler, self).tick(event_t=event_t,
                                                            min=min,
                                                            heappop=heapq.heappop,
                                                            heappush=heapq.heappush)
