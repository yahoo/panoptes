"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
from yahoo_panoptes.framework.exceptions import PanoptesBaseException


class PanoptesSNMPException(PanoptesBaseException):
    """The base Easy SNMP exception which covers all exceptions raised."""
    pass


class PanoptesSNMPConnectionException(PanoptesSNMPException):
    """Indicates a problem connecting to the remote host."""
    pass


class PanoptesSNMPTimeoutException(PanoptesSNMPConnectionException):
    """Raised when an SNMP request times out."""
    pass


class PanoptesSNMPUnknownObjectIDException(PanoptesSNMPException):
    """Raised when an inexisted OID is requested."""
    pass


class PanoptesSNMPNoSuchNameException(PanoptesSNMPException):
    """
    Raised when an OID is requested which may be an invalid object name
    or invalid instance (only applies to SNMPv1).
    """
    pass


class PanoptesSNMPNoSuchObjectException(PanoptesSNMPException):
    """
    Raised when an OID is requested which may have some form of existence but
    an invalid object name.
    """
    pass


class PanoptesSNMPNoSuchInstanceException(PanoptesSNMPException):
    """
    Raised when a particular OID index requested from Net-SNMP doesn't exist.
    """
    pass


class PanoptesSNMPUndeterminedTypeException(PanoptesSNMPException):
    """
    Raised when the type cannot be determine when setting the value of an OID.
    """
    pass
