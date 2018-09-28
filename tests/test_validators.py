"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""

import time
import unittest

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
