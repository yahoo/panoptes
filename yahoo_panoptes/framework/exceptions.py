"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module defines the 'base' exception for the Panoptes system
"""


class PanoptesBaseException(Exception):
    """
    The base class for all errors in the Panoptes system
    """
    def __init__(self, *args, **kwargs):
        super(PanoptesBaseException, self).__init__(self, *args, **kwargs)
