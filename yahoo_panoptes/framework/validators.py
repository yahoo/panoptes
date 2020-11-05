"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module contains custom validators which can be used in assert checks
"""
from builtins import object
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
    def valid_logger(cls, logger):
        """
        Validate a logger

        Args:
            cls: (todo): write your description
            logger: (todo): write your description
        """
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
        return callback and hasattr(callback, u'__call__')

    @classmethod
    def valid_hashable_object(cls, object_to_check):
        """
        Checks if the passed object is hashable

        Args:
            object_to_check (object): The object to check

        Returns:
            bool: True if the object is hashable
        """

        return hasattr(object_to_check, u'__hash__') and object_to_check.__hash__

    @classmethod
    def valid_port(cls, port):
        """
        Return the port number.

        Args:
            cls: (todo): write your description
            port: (int): write your description
        """
        return type(port) in integer_types and (port > 0) and (port < 65536)

    @classmethod
    def valid_number(cls, value):
        """
        Validates that value.

        Args:
            cls: (todo): write your description
            value: (todo): write your description
        """
        return isinstance(value, numbers.Number) and not isinstance(value, bool)

    @classmethod
    def valid_nonzero_integer(cls, value):
        """
        Validate that value is a valid integer.

        Args:
            cls: (todo): write your description
            value: (todo): write your description
        """
        return type(value) in integer_types and (value > 0)

    @classmethod
    def valid_positive_integer(cls, value):
        """
        N.b. In Panoptes, '0' is considered a positive integer.
        """
        return type(value) in integer_types and (value > -1)

    @classmethod
    def valid_none_or_string(cls, value):
        """
        Validates that value is none if not none.

        Args:
            cls: (todo): write your description
            value: (str): write your description
        """
        return (value is None) or cls.valid_nonempty_string(value)

    @classmethod
    def valid_nonempty_string(cls, value):
        """
        Validate that value is a valid string.

        Args:
            cls: (todo): write your description
            value: (str): write your description
        """
        return isinstance(value, string_types) and (len(value) > 0)

    @classmethod
    def valid_none_or_nonempty_string(cls, value):
        """
        Checks if the passed value is a None or non-empty string

        Args:
            value (object): The value to check

        Returns:
            bool: True if the validation passes, false otherwise
        """
        return value is None or cls.valid_nonempty_string(value)

    @classmethod
    def valid_readable_path(cls, path):
        """
        Check if a path is readable

        Args:
            cls: (todo): write your description
            path: (str): write your description
        """
        try:
            return True if (os.path.isdir(path) and os.access(path, os.R_OK)) else False
        except:
            return False

    @classmethod
    def valid_readable_file(cls, filename):
        """
        Check if a file is readable.

        Args:
            cls: (todo): write your description
            filename: (str): write your description
        """
        try:
            return True if (os.path.isfile(filename) and os.access(filename, os.R_OK)) else False
        except:
            return False

    @classmethod
    def valid_nonempty_iterable_of_strings(cls, value):
        """
        Validate that all non - place is iterable.

        Args:
            cls: (todo): write your description
            value: (str): write your description
        """

        return hasattr(value, u'__iter__') and \
               hasattr(value, u'__len__') and \
               len(value) > 0 and \
               all(cls.valid_nonempty_string(val) for val in value) and \
               all(len(val) > 0 for val in value) and \
               not isinstance(value, str)

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
        """
        Validate a snmp oid.

        Args:
            cls: (todo): write your description
            oid: (str): write your description
        """
        return True if NUMERIC_OID.match(oid) else False
