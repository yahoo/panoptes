"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
from yahoo_panoptes_snmp.compat import urepr
from yahoo_panoptes_snmp.utils import strip_non_printable

from yahoo_panoptes.framework.validators import *


class PanoptesSNMPVariable(object):
    def __init__(self, queried_oid, oid, index, value, snmp_type):
        """
        Initialize snmp oid.

        Args:
            self: (todo): write your description
            queried_oid: (todo): write your description
            oid: (int): write your description
            index: (int): write your description
            value: (todo): write your description
            snmp_type: (todo): write your description
        """
        assert PanoptesValidators.valid_numeric_snmp_oid(str(queried_oid)), \
            u'queried_oid must be numeric string with a leading period'
        assert PanoptesValidators.valid_numeric_snmp_oid(str(oid)), u'oid must be numeric string with a leading period'
        assert PanoptesValidators.valid_nonempty_string(str(index)), u'index must be None or a non-empty string'
        assert PanoptesValidators.valid_nonempty_string(str(snmp_type)), u'snmp_type must be a non-empty string'

        self._queried_oid = queried_oid
        self._index = self._normalize_index(oid, index, queried_oid)
        self._value = value
        self._snmp_type = snmp_type

    def __repr__(self):
        """
        Return a representation of this object.

        Args:
            self: (todo): write your description
        """

        printable_value = self.value.decode(u'ascii', u'backslashreplace') if hasattr(self.value, u'decode') \
            else self.value

        return (
            u"<{0} value={1} (oid={2}, index={3}, snmp_type={4})>".format(
                    self.__class__.__name__,
                    urepr(printable_value), urepr(self.oid),
                    urepr(self._index), urepr(self._snmp_type)
            )
        )

    @property
    def oid(self):
        """
        : return : oid - oid

        Args:
            self: (todo): write your description
        """
        return self._queried_oid

    @property
    def index(self):
        """
        : return : index.

        Args:
            self: (todo): write your description
        """
        return self._index

    @property
    def value(self):
        """
        Returns the value of the field.

        Args:
            self: (todo): write your description
        """
        return self._value

    @property
    def snmp_type(self):
        """
        The snmp type.

        Args:
            self: (todo): write your description
        """
        return self._snmp_type

    @staticmethod
    def _normalize_index(oid, index, queried_oid):
        """
        Parses the provided oid to return both the prefix and the index

        Args:
            oid (str): The OID (as a dotted numerical string) to parse
            index (str): The index, if known
            queried_oid (str): The OID (as a dotted numerical string) which was queried
        Returns:
            tuple: the parsed prefix and index

        """
        prefix = oid[:len(queried_oid)]
        # TODO: This is hacky and should probably be fixed in the normalize_oid function of easysnmp
        if oid != prefix:
            index = oid[len(queried_oid) + 1:] + '.' + index

        return index
