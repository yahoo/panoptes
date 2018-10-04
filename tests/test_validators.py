"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""

import time
import unittest
import logging
import os

from yahoo_panoptes.framework.validators import PanoptesValidators


class TestValidators(unittest.TestCase):
    def test_valid_nonempty_iterable_of_strings(self):
        class SampleIterator(object):
            def __init__(self, item):
                self.test_list = list()
                self.test_list.append(item)

            def __iter__(self):
                return iter(self.test_list)

            def __len__(self):
                return len(self.test_list)

        self.assertTrue(PanoptesValidators.valid_nonempty_iterable_of_strings(list('x')))
        self.assertTrue(PanoptesValidators.valid_nonempty_iterable_of_strings(set('x')))
        self.assertTrue(PanoptesValidators.valid_nonempty_iterable_of_strings(SampleIterator('x')))
        self.assertFalse(PanoptesValidators.valid_nonempty_iterable_of_strings('x'))
        self.assertFalse(PanoptesValidators.valid_nonempty_iterable_of_strings([]))
        self.assertFalse(PanoptesValidators.valid_nonempty_iterable_of_strings(['']))
        self.assertFalse(PanoptesValidators.valid_nonempty_iterable_of_strings(['x', '']))
        self.assertFalse(PanoptesValidators.valid_nonempty_iterable_of_strings(['x', set('x')]))

    def test_valid_timestamp(self):
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

    def test_valid_plugin_result_class(self):
        self.assertFalse(PanoptesValidators.valid_plugin_result_class(None))
        self.assertTrue(PanoptesValidators.valid_plugin_result_class(str))

    def test_valid_logger(self):
        self.assertFalse(PanoptesValidators.valid_logger(None))
        self.assertTrue(PanoptesValidators.valid_logger(logging.Logger("test_logger")))

    def test_valid_callback(self):
        self.assertFalse(PanoptesValidators.valid_callback(object()))
        self.assertTrue(PanoptesValidators.valid_callback(object.__init__))

    def test_valid_hashable_object(self):
        self.assertTrue(PanoptesValidators.valid_hashable_object(object()))
        self.assertFalse(PanoptesValidators.valid_hashable_object(list()))

    def test_valid_port(self):
        self.assertTrue(PanoptesValidators.valid_port(1L))
        self.assertFalse(PanoptesValidators.valid_port(1.0))
        self.assertFalse(PanoptesValidators.valid_port(65540))
        self.assertFalse(PanoptesValidators.valid_port(0))
        self.assertFalse(PanoptesValidators.valid_port(-1))
        self.assertFalse(PanoptesValidators.valid_port("0"))
        self.assertFalse(PanoptesValidators.valid_port("1"))
        self.assertFalse(PanoptesValidators.valid_port(True))
        self.assertFalse(PanoptesValidators.valid_port(False))

    def test_valid_number(self):
        self.assertFalse(PanoptesValidators.valid_number(False))
        self.assertFalse(PanoptesValidators.valid_number("1.234"))
        self.assertTrue(PanoptesValidators.valid_number(2 ** (2 ** (2 ** (2 ** 2)))))
        self.assertTrue(PanoptesValidators.valid_number(1J))

    def test_valid_nonzero_integer(self):
        self.assertFalse(PanoptesValidators.valid_nonzero_integer(-1))
        self.assertFalse(PanoptesValidators.valid_nonzero_integer(0))
        self.assertFalse(PanoptesValidators.valid_nonzero_integer(-0L))
        self.assertFalse(PanoptesValidators.valid_nonzero_integer(1.0))
        self.assertFalse(PanoptesValidators.valid_nonzero_integer(""))
        self.assertFalse(PanoptesValidators.valid_nonzero_integer(False))
        self.assertFalse(PanoptesValidators.valid_nonzero_integer(True))
        self.assertTrue(PanoptesValidators.valid_nonzero_integer(2 ** (2 ** (2 ** (2 ** 2)))))

    def test_valid_positive_integer(self):
        self.assertTrue(PanoptesValidators.valid_positive_integer(0L))
        self.assertTrue(PanoptesValidators.valid_positive_integer(2 ** (2 ** (2 ** (2 ** 2)))))
        self.assertFalse(PanoptesValidators.valid_positive_integer(False))
        self.assertFalse(PanoptesValidators.valid_positive_integer(True))
        self.assertFalse(PanoptesValidators.valid_positive_integer(""))
        self.assertFalse(PanoptesValidators.valid_positive_integer(1.0))
        self.assertFalse(PanoptesValidators.valid_positive_integer(-5))
        self.assertTrue(PanoptesValidators.valid_positive_integer(-0L))

    def test_valid_nonempty_string(self):
        self.assertFalse(PanoptesValidators.valid_nonempty_string(""))
        self.assertTrue(PanoptesValidators.valid_nonempty_string("hello world"))
        self.assertFalse(PanoptesValidators.valid_nonempty_string(0))
        
    def test_valid_readable_path(self):
        self.assertTrue(PanoptesValidators.valid_readable_path(os.getcwd()))
        self.assertFalse(PanoptesValidators.valid_readable_path(os.getcwd() + '/test_dir'))
        self.assertFalse(PanoptesValidators.valid_readable_path('not a path'))
        self.assertFalse(PanoptesValidators.valid_readable_path(None))

    def test_valid_readable_file(self):
        self.assertTrue(PanoptesValidators.valid_readable_file(os.path.realpath(__file__)))
        self.assertFalse(PanoptesValidators.valid_readable_file(None))

    def test_valid_numeric_snmp_oid(self):
        self.assertTrue(PanoptesValidators.valid_numeric_snmp_oid(".1.3.6.1.4.1.2636.3.1.13.1.6.2.1.0.0"))
        self.assertFalse(PanoptesValidators.valid_numeric_snmp_oid("1.3.6.1.4.1.2636.3.1.13.1.6.2.1.0.0"))
