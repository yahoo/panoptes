"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module contains custom validators which can be used in assert checks
"""
import os
import time
import numbers
import re
import logging

from six import string_types, integer_types

NUMERIC_OID = re.compile(r'\.(\d+)(\.\d+)*')


class PanoptesValidators(object):
    """
    This class contains custom validator methods
    """

    @classmethod
    def valid_plugin_result_class(cls, plugin_result_class):
        """
        valid_plugin_result_class(cls, plugin_result_class)

        Checks if the passed class is a subclass of object class
        Args:
            plugin_result_class (object): The class to check

        Returns:
            bool: True if the class is not null and is an subclass of object
        """
        return plugin_result_class and issubclass(plugin_result_class, object)

    @classmethod
    def valid_logger(cls, logger):
        return isinstance(logger, logging.Logger)

    @classmethod
    def valid_callback(cls, callback):
        """
        valid_callback(cls, callback)

        Checks if the passed object is a callable
        Args:
            callback (callback): The object to check

        Returns:
            bool: True if the object passed is a callable

        """
        return callback and hasattr(callback, '__call__')

    @classmethod
    def valid_hashable_object(cls, object_to_check):
        """
        Checks if the passed object is hashable

        Args:
            object_to_check (object): The object to check

        Returns:
            bool: True if the object is hashable
        """

        return hasattr(object_to_check, '__hash__') and object_to_check.__hash__

    @classmethod
    def valid_port(cls, port):
        return type(port) in integer_types and (port > 0) and (port < 65536)

    @classmethod
    def valid_number(cls, value):
        return isinstance(value, numbers.Number) and not isinstance(value, bool)

    @classmethod
    def valid_nonzero_integer(cls, value):
        return type(value) in integer_types and (value > 0)

    @classmethod
    def valid_positive_integer(cls, value):
        """
        N.b. In Panoptes, '0' is considered a positive integer.
        """
        return type(value) in integer_types and (value > -1)

    @classmethod
    def valid_nonempty_string(cls, value):
        return isinstance(value, string_types) and (len(value) > 0)

    @classmethod
    def valid_readable_path(cls, path):
        try:
            return True if (os.path.isdir(path) and os.access(path, os.R_OK)) else False
        except:
            return False

    @classmethod
    def valid_readable_file(cls, filename):
        try:
            return True if (os.path.isfile(filename) and os.access(filename, os.R_OK)) else False
        except:
            return False

    @classmethod
    def valid_nonempty_iterable_of_strings(cls, value):
        return hasattr(value, '__iter__') and \
               hasattr(value, '__len__') and \
               len(value) > 0 and \
               all(type(val) == str for val in value) and \
               all(len(val) > 0 for val in value)

    @classmethod
    def valid_timestamp(cls, timestamp, max_age=604800, max_skew=60):
        """
        Checks is the passed value is a valid unix timestamp, which is not older than max_age (defaults to 7 days) and\
        not more than max_skew (defaults to 60 seconds)

        max_skew is needed to handle distributed systems not have exactly synced clocks

        Args:
            timestamp (int, float): An integer or float which contains the timestamp
            max_age (int, float): The maximum age, in seconds, of how old the timestamp can be from the current time
            max_skew (int, float): The maximum skew (seconds in the future) to how much more the timestamp can be from\
            the current time

        Returns:
            bool: True if the validation passes, false otherwise

        """
        return type(timestamp) in [int, float] and \
            type(max_age) in [int, float] and \
            type(max_skew) in [int, float] and \
            (time.time() - max_age) <= timestamp <= (time.time() + max_skew)

    @classmethod
    def valid_numeric_snmp_oid(cls, oid):
        return True if NUMERIC_OID.match(oid) else False
