"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

Base mib class
"""
from builtins import object
from yahoo_panoptes.framework.metrics import PanoptesMetricType


class SNMPTypeMixin(object):
    """Mixin class for SNMP metrics and variables"""
    name = u'float'
    metric_type = PanoptesMetricType.GAUGE
    valid_types = float

    def __init__(self, value):
        """
        Initialize the value

        Args:
            self: (todo): write your description
            value: (todo): write your description
        """
        self.value = value
        self.validate()

    def __str__(self):
        """
        Return the string representation of this object.

        Args:
            self: (todo): write your description
        """
        return self.name

    def validate(self):
        """
        Validate that the value is valid

        Raises
        ------
        ValueError - The value is invalid
        """
        self.validate_type()
        self.validate_value()

    def validate_type(self):
        """
        Verify the value is the right type

        Raises
        ------
        ValueError - The self.value is not the right type
        """
        if not isinstance(self.value, self.valid_types):
            raise ValueError(u'%r is not a %s' % (self.value, type(self.valid_types)))

    def validate_value(self):
        """
        Validate the value of value is valid

        Raises
        ------
        ValueError - The self.value is not valid
        """
        pass


class SNMPType(SNMPTypeMixin):
    pass


class SNMPString(str, SNMPTypeMixin):
    pass


class SNMPInteger(int, SNMPTypeMixin):
    valid_types = int
    name = 'integer'
    metric_type = PanoptesMetricType.COUNTER


class SNMPInteger32(SNMPInteger):
    def validate_value(self):
        """
        Validate that the field

        Args:
            self: (todo): write your description
        """
        if self.value > (2 ** 31) - 1:
            raise ValueError(u'32 bit integer overflow, value of %r is to large for a signed 32 bit integer' %
                             self.value)


class SNMPGauge32(SNMPInteger32):
    metric_type = PanoptesMetricType.GAUGE


class SNMPFloat(float, SNMPTypeMixin):
    pass


class oid(object):
    """
    An mib oid class
    """
    def __init__(self, value, snmp_type=None, description=None):
        """
        Parameters
        ----------
        value: str
            Initial oid string value
        """
        self.value = value
        self.snmp_type = snmp_type
        self.description = description

    @property
    def oid(self):
        """
        Return oid : class.

        Args:
            self: (todo): write your description
        """
        return self.value

    def __repr__(self):
        """
        Return a human - readable representation of this object.

        Args:
            self: (todo): write your description
        """
        return u'oid("' + str(self.value) + u'")'

    def __str__(self):
        """
        Returns the string representation of the string.

        Args:
            self: (todo): write your description
        """
        return self.value

    def __add__(self, item):
        """
        Add a snmp item to the snmp_type.

        Args:
            self: (todo): write your description
            item: (todo): write your description
        """
        try:
            snmp_type = item.snmp_type
        except AttributeError:
            snmp_type = self.snmp_type
        item = str(item).strip(u'.')
        return oid(str(self.value) + u'.' + item, snmp_type=snmp_type)


class Mib(object):
    """
    Base Mib class
    """
    pass
