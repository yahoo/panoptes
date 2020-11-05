"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module implements a 'Tour Of Duty' class that help a calling process determine when it's time to cleanup and exit.
Tour Of Duty can be based on the number of tasks, elapsed time or memory used
"""

from builtins import object
import time
import platform
import random
import resource

from yahoo_panoptes.framework.validators import PanoptesValidators


class PanoptesTourOfDuty(object):
    """
    A class that help a calling process determine when it's time to cleanup and exit.

    'Tour Of Duty' can be based on the number of tasks, elapsed time or memory used

    Args:
        tasks (int): The number of tasks after which the Tour Of Duty is considered complete
        seconds (int): The elapsed time, in seconds, after which the Tour Of Duty is considered complete
        memory_growth_mb (int): The amount of _additional_ memory, in megabytes, after which the Tour Of Duty is \
        considered complete
        splay_percent (float): A percentage by which to randomize all the above values - this is to prevent multiple \
        similar processes all completing their Tour Of Duty at the exact same instance
    """
    def _get_memory_utilization_in_mb(self):
        """
        Gets memory memory size.

        Args:
            self: (todo): write your description
        """
        return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss // self._memory_divider

    def __init__(self, tasks=1000, seconds=14400, memory_growth_mb=200, splay_percent=0):
        """
        Initialize the memory.

        Args:
            self: (todo): write your description
            tasks: (str): write your description
            seconds: (int): write your description
            memory_growth_mb: (todo): write your description
            splay_percent: (int): write your description
        """
        assert PanoptesValidators.valid_nonzero_integer(tasks), u'tasks must a integer greater than zero'
        assert PanoptesValidators.valid_nonzero_integer(seconds), u'seconds must a integer greater than zero'
        assert PanoptesValidators.valid_nonzero_integer(
            memory_growth_mb), u'memory_growth_mb must a integer greater than zero'
        assert PanoptesValidators.valid_number(splay_percent), u'splay_percent must a number'
        assert 0 <= splay_percent <= 100, u'splay_percent must be a number between 0 and 100, inclusive'

        self._start_time = time.time()

        if platform.system() == u'Linux':
            # On Linux platforms, memory is reported in kilobytes
            self._memory_divider = 1024
        else:
            # On all other platforms, assume bytes (true for Mac OS X)
            self._memory_divider = 1048576

        self._initial_memory_mb = self._get_memory_utilization_in_mb()
        self._task_count = 0

        self._tasks = tasks
        self._seconds = seconds
        self._memory_growth_mb = memory_growth_mb
        self._splay_percent = float(splay_percent) / 100
        self._adjusted_tasks = round(self._tasks * (1 + (random.random() * self._splay_percent)))
        self._adjusted_seconds = round(self._seconds * (1 + (random.random() * self._splay_percent)))
        self._adjusted_memory_growth_mb = round(self._memory_growth_mb * (1 + (random.random() * self._splay_percent)))

    @property
    def adjusted_tasks(self):
        """
        Return a : class : return :

        Args:
            self: (todo): write your description
        """
        return self._adjusted_tasks

    @property
    def adjusted_seconds(self):
        """
        Return the number of seconds since seconds.

        Args:
            self: (todo): write your description
        """
        return self._adjusted_seconds

    @property
    def adjusted_memory_growth_mb(self):
        """
        Gets the number of neighbor.

        Args:
            self: (todo): write your description
        """
        return self._adjusted_memory_growth_mb

    @property
    def tasks_completed(self):
        """
        Return a list of tasks that have been executed.

        Args:
            self: (todo): write your description
        """
        return self._task_count >= self.adjusted_tasks

    @property
    def time_completed(self):
        """
        The number of seconds.

        Args:
            self: (todo): write your description
        """
        return (time.time() - self._start_time) > self.adjusted_seconds

    @property
    def memory_growth_completed(self):
        """
        The number of the number of memory.

        Args:
            self: (todo): write your description
        """
        return (self._get_memory_utilization_in_mb() - self._initial_memory_mb) > self.adjusted_memory_growth_mb

    @property
    def iterations(self):
        """
        Returns the number of iterations the scheduler has completed

        Returns:
            int
        """
        return self._task_count

    def increment_task_count(self):
        """
        Increments the task count which counts towards the Tour Of Duty

        Returns:
            None
        """
        self._task_count += 1

    @property
    def completed(self):
        """
        Signifies if your watch has ended

        Returns:
            bool
        """
        return self.tasks_completed or self.time_completed or self.memory_growth_completed
