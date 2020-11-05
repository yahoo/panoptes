"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
from builtins import object
from builtins import str

import time
import unittest
import logging
import os

from yahoo_panoptes.framework.validators import PanoptesValidators


class TestValidators(unittest.TestCase):
    def test_valid_nonempty_iterable_of_strings(self):
        """
        Return an iterable items in the iterable.

        Args:
            self: (todo): write your description
        """
        class SampleIterator(object):
            def __init__(self, item):
                """
                Add test list of items to the list

                Args:
                    self: (todo): write your description
                    item: (todo): write your description
                """
                self.test_list = list()
                self.test_list.append(item)

            def __iter__(self):
                """
                Return an iterable of all the elements.

                Args:
                    self: (todo): write your description
                """
                return iter(self.test_list)

            def __len__(self):
                """
                Returns the length of the list.

                Args:
                    self: (todo): write your description
                """
                return len(self.test_list)

        self.assertTrue(PanoptesValidators.valid_nonempty_iterable_of_strings(list(u'x')))
        self.assertTrue(PanoptesValidators.valid_nonempty_iterable_of_strings(set(u'x')))
        self.assertTrue(PanoptesValidators.valid_nonempty_iterable_of_strings(SampleIterator(u'x')))
        self.assertFalse(PanoptesValidators.valid_nonempty_iterable_of_strings(u'x'))
        self.assertFalse(PanoptesValidators.valid_nonempty_iterable_of_strings([]))
        self.assertFalse(PanoptesValidators.valid_nonempty_iterable_of_strings([u'']))
        self.assertFalse(PanoptesValidators.valid_nonempty_iterable_of_strings([u'x', u'']))
        self.assertFalse(PanoptesValidators.valid_nonempty_iterable_of_strings([u'x', set(u'x')]))

    def test_valid_timestamp(self):
        """
        Test for valid timestamp and validates of the validators.

        Args:
            self: (todo): write your description
        """
        self.assertTrue(PanoptesValidators.valid_timestamp(timestamp=time.time()))
        self.assertTrue(PanoptesValidators.valid_timestamp(timestamp=time.time() - 86400))
        self.assertTrue(PanoptesValidators.valid_timestamp(timestamp=time.time() + 10))
        self.assertTrue(PanoptesValidators.valid_timestamp(timestamp=time.time() + 70, max_skew=120))
        self.assertFalse(PanoptesValidators.valid_timestamp(timestamp=0))
        self.assertFalse(PanoptesValidators.valid_timestamp(timestamp=time.time() + 120))
        self.assertFalse(PanoptesValidators.valid_timestamp(timestamp=time.time() - 604810))
        self.assertFalse(PanoptesValidators.valid_timestamp(timestamp=None))
        self.assertFalse(PanoptesValidators.valid_timestamp(timestamp=time.time(), max_age=None))
        self.assertFalse(PanoptesValidators.valid_timestamp(timestamp=time.time(), max_skew=None))

    def test_valid_logger(self):
        """
        Test if the test is validators.

        Args:
            self: (todo): write your description
        """
        self.assertFalse(PanoptesValidators.valid_logger(None))
        self.assertTrue(PanoptesValidators.valid_logger(logging.Logger(u"test_logger")))

    def test_valid_callback(self):
        """
        Perform validation on_validator

        Args:
            self: (todo): write your description
        """
        self.assertFalse(PanoptesValidators.valid_callback(object()))
        self.assertTrue(PanoptesValidators.valid_callback(object.__init__))

    def test_valid_hashable_object(self):
        """
        Validate that all validators are valid.

        Args:
            self: (todo): write your description
        """
        self.assertTrue(PanoptesValidators.valid_hashable_object(object()))
        self.assertFalse(PanoptesValidators.valid_hashable_object(list()))

    def test_valid_port(self):
        """
        Perform the port and port.

        Args:
            self: (todo): write your description
        """
        self.assertTrue(PanoptesValidators.valid_port(1))
        self.assertFalse(PanoptesValidators.valid_port(1.0))
        self.assertFalse(PanoptesValidators.valid_port(65540))
        self.assertFalse(PanoptesValidators.valid_port(0))
        self.assertFalse(PanoptesValidators.valid_port(-1))
        self.assertFalse(PanoptesValidators.valid_port(u"0"))
        self.assertFalse(PanoptesValidators.valid_port(u"1"))
        self.assertFalse(PanoptesValidators.valid_port(True))
        self.assertFalse(PanoptesValidators.valid_port(False))

    def test_valid_number(self):
        """
        Validate that the test is valid.

        Args:
            self: (todo): write your description
        """
        self.assertFalse(PanoptesValidators.valid_number(False))
        self.assertFalse(PanoptesValidators.valid_number(u"1.234"))
        self.assertTrue(PanoptesValidators.valid_number(2 ** (2 ** (2 ** (2 ** 2)))))
        self.assertTrue(PanoptesValidators.valid_number(1J))

    def test_valid_nonzero_integer(self):
        """
        Validate that the non - zero.

        Args:
            self: (todo): write your description
        """
        self.assertFalse(PanoptesValidators.valid_nonzero_integer(-1))
        self.assertFalse(PanoptesValidators.valid_nonzero_integer(0))
        self.assertFalse(PanoptesValidators.valid_nonzero_integer(-0))
        self.assertFalse(PanoptesValidators.valid_nonzero_integer(1.0))
        self.assertFalse(PanoptesValidators.valid_nonzero_integer(u""))
        self.assertFalse(PanoptesValidators.valid_nonzero_integer(False))
        self.assertFalse(PanoptesValidators.valid_nonzero_integer(True))
        self.assertTrue(PanoptesValidators.valid_nonzero_integer(2 ** (2 ** (2 ** (2 ** 2)))))

    def test_valid_positive_integer(self):
        """
        Validate that the positive integer is valid.

        Args:
            self: (todo): write your description
        """
        self.assertTrue(PanoptesValidators.valid_positive_integer(0))
        self.assertTrue(PanoptesValidators.valid_positive_integer(2 ** (2 ** (2 ** (2 ** 2)))))
        self.assertFalse(PanoptesValidators.valid_positive_integer(False))
        self.assertFalse(PanoptesValidators.valid_positive_integer(True))
        self.assertFalse(PanoptesValidators.valid_positive_integer(u""))
        self.assertFalse(PanoptesValidators.valid_positive_integer(1.0))
        self.assertFalse(PanoptesValidators.valid_positive_integer(-5))
        self.assertTrue(PanoptesValidators.valid_positive_integer(-0))

    def test_valid_nonempty_string(self):
        """
        Test that the nonempty string.

        Args:
            self: (todo): write your description
        """
        self.assertFalse(PanoptesValidators.valid_nonempty_string(u""))
        self.assertTrue(PanoptesValidators.valid_nonempty_string(u"hello world"))
        self.assertFalse(PanoptesValidators.valid_nonempty_string(0))

    def test_valid_readable_path(self):
        """
        Test if the path is valid.

        Args:
            self: (todo): write your description
        """
        self.assertTrue(PanoptesValidators.valid_readable_path(os.getcwd()))
        self.assertFalse(PanoptesValidators.valid_readable_path(os.getcwd() + u'/test_dir'))
        self.assertFalse(PanoptesValidators.valid_readable_path(u'not a path'))
        self.assertFalse(PanoptesValidators.valid_readable_path(None))

    def test_valid_readable_file(self):
        """
        Check if the file is valid.

        Args:
            self: (todo): write your description
        """
        self.assertTrue(PanoptesValidators.valid_readable_file(os.path.realpath(__file__)))
        self.assertFalse(PanoptesValidators.valid_readable_file(None))

    def test_valid_numeric_snmp_oid(self):
        """
        Validate that the snmp oid is valid.

        Args:
            self: (todo): write your description
        """
        self.assertTrue(PanoptesValidators.valid_numeric_snmp_oid(u".1.3.6.1.4.1.2636.3.1.13.1.6.2.1.0.0"))
        self.assertFalse(PanoptesValidators.valid_numeric_snmp_oid(u"1.3.6.1.4.1.2636.3.1.13.1.6.2.1.0.0"))
