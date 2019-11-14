"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module holds various helper functions used throughout the system
"""
from __future__ import division
from future import standard_library
from builtins import hex
from builtins import range
from past.utils import old_div
import ctypes
import inspect
import logging
import os
import platform
import threading
import uuid
import gevent
import ipaddress
from _socket import gaierror, herror
try:
    from cStringIO import StringIO
except ImportError:
    from io import StringIO

from yahoo_panoptes.framework.exceptions import PanoptesBaseException
from yahoo_panoptes.framework import validators

from configobj import ConfigObj, ConfigObjError, flatten_errors
from validate import Validator
from gevent import socket
from gevent.util import wrap_errors

standard_library.install_aliases()
import re  # noqa
import sys  # noqa

LOG = logging.getLogger(__name__)


def is_python_2():
    """
    Returns true if the current python environment is running python2.
    Returns:
        bool: True if python version is 2
    """
    return sys.version_info[0] == 2


def normalize_plugin_name(plugin_name):
    """
    Return the normalized plugin name so that they can be used safely throughout the system

    The aim of this function is to replace any 'unsafe' characters in the plugin name with characters that would be safe
    across all the supporting services like Redis, ZooKeeper, Kafka etc.

    Args:
        plugin_name (str): The plugin name to normalize

    Returns:
        str: The normalized plugin name

    """
    assert validators.PanoptesValidators.valid_nonempty_string(plugin_name), u'plugin_name must be a non-empty str'
    temp_plugin_name = plugin_name.replace(u'_', u'__')
    normalized_plugin_name = re.sub(r'[^A-Za-z0-9_]', u'_', temp_plugin_name)
    return normalized_plugin_name


def get_module_mtime(module_path):
    """
    Return the unix mtime of a module

    If the module path is a single file, then the mtime of the file is returned
    If the module path is a directory then the mtime of the file with the highest mtime in the directory would be
    returned

    Args:
        module_path (str): The absolute directory path of the module, without the '.py' extension

    Returns:
        int: The mtime of the module

    """
    mtime = 0

    assert validators.PanoptesValidators.valid_nonempty_string(module_path), u'module_path must be a non-empty str'
    if os.path.isdir(module_path):
        for f in os.listdir(module_path):
            f_time = int(os.path.getmtime(module_path + u'/' + f))
            mtime = f_time if f_time > mtime else mtime
    elif os.path.isfile(module_path + u'.py'):
        mtime = int(os.path.getmtime(module_path + u'.py'))

    return mtime


def resolve_hostnames(hostnames, timeout):
    """
    Do DNS resolution for a given list of hostnames

    This function uses gevent to resolve all the hostnames in *parallel*

    Args:
        hostnames (list): A list of strings
        timeout (int): The number of seconds to wait for resolution of **all** hostnames

    Returns:
        list: A list of (hostname, address) tuples in the same order as the input list of hostnames

    """
    assert validators.PanoptesValidators.valid_nonempty_iterable_of_strings(hostnames), u'hostnames should be a list'
    assert validators.PanoptesValidators.valid_nonzero_integer(timeout), u'timeout should be an int greater than zero'

    jobs = [gevent.spawn(wrap_errors(gaierror, socket.gethostbyname), host) for host in hostnames]
    gevent.joinall(jobs, timeout=timeout)
    addresses = [job.value if not isinstance(job.get(), gaierror) else None for job in jobs]
    results = [(hostnames[i], result) for i, result in enumerate(addresses)]
    return results


def unknown_hostname(ip):
    """
    Returns a custom  hostname for an unresolvable IP

    Args:
        ip (str): The unresolved IP for which to craft the hostname for

    Returns:
        str: The hostname returned is of the format: unknown-x-x-x-x

    """

    return u'unknown-' + re.sub(r'[.:]', u'-', ip)


def get_hostnames(ips, timeout):
    """
    Do DNS resolution for a given list of IPs

    Args:
        ips (list): A list of IPs
        timeout (int): The number of seconds to wait for resolution of **all** IPs

    Returns:
        list: A list of (address, hosname) tuples in the same order as the input list of IPs
    """
    assert validators.PanoptesValidators.valid_nonempty_iterable_of_strings(ips), u'ips should be a list'
    assert validators.PanoptesValidators.valid_nonzero_integer(timeout), u'timeout should be an int greater than zero'

    jobs = [gevent.spawn(wrap_errors((gaierror, herror), socket.gethostbyaddr), ip) for ip in ips]
    gevent.joinall(jobs, timeout=timeout)
    hostnames = [None if isinstance(job.get(), (gaierror, herror)) else job.value for job in jobs]
    results = {
        ips[i]: unknown_hostname(ips[i]) if ((not result) or
                                             (not result[0]) or
                                             result[0].startswith(u'UNKNOWN'))
        else result[0]
        for i, result in enumerate(hostnames)}
    return results


def get_ip_version(ip):
    # CR: http://stackoverflow.com/questions/11827961/checking-for-ip-addresses

    try:
        socket.inet_aton(ip)
        return 4
    except socket.error:
        pass
    try:
        socket.inet_pton(socket.AF_INET6, ip)
        return 6
    except socket.error:
        pass
    raise ValueError(ip)


def get_hostname():
    """
    Get the hostname of the current host

    Returns:
        str: The hostname

    """
    return str(platform.node())


def get_os_tid():
    """
    Get the Linux process id associated with the current thread

    Returns:
        int: The process id

    """
    if sys.platform.startswith(u'linux'):
        return ctypes.CDLL(u'libc.so.6').syscall(186)
    else:
        # TODO: This is hacky - we need to replace it with something that actually returns the OS thread ID
        if is_python_2():
            return threading._get_ident()
        else:
            return threading.get_ident()


def get_calling_module_name(depth=3):
    """
    Get the name of the 'calling' module

    Args:
        depth (int): How many levels to look 'up' from the current module. Remember that the enclosing 'helpers' \
        module is counted as 1.

    Returns:
        str: The calling module name

    """
    assert isinstance(depth, int), u'depth should be an int'
    frame = inspect.stack()[depth]
    LOG.debug(u'Got calling frame %r', frame)
    module = inspect.getmodule(frame[0])
    if module:
        return module.__name__


def inspect_calling_module_for_name(name):
    """
    Python 3 only!
    Inspects the stack to check if `name` is in the filename of a frame

    Returns:
        bool: True if the `name` is in the filename of a frame.
    """
    if is_python_2():
        return False

    for frame in inspect.stack():
        if hasattr(frame, 'filename'):
            if name in frame.filename:
                return True
    return False


def get_client_id(prefix):
    """
    Generates a client id, based on the given prefix

    Args:
        prefix (str): A non-empty str

    Returns:
        str: The client id which consists of the prefix combined with the hostname and thread id, separated by '_'
    """
    assert validators.PanoptesValidators.valid_nonempty_string(prefix), u'prefix must be a non-empty str'
    return u'_'.join([str(uuid.uuid4()), prefix, get_hostname(), str(get_os_tid())])


class CaptureStdErr(list):
    def __enter__(self):
        self._stderr = sys.stderr
        sys.stderr = self._stringio = StringIO()
        return self

    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        sys.stderr = self._stderr


class CaptureStdOut(list):
    def __enter__(self):
        self._stdout = sys.stdout
        sys.stdout = self._stringio = StringIO()
        return self

    def __exit__(self, *args):
        self.extend(self._stringio.getvalue().splitlines())
        sys.stdout = self._stdout


class PanoptesConfigurationParsingError(PanoptesBaseException):
    pass


def parse_config_file(config_file, config_spec_file):

    assert validators.PanoptesValidators.valid_nonempty_string(config_file), u'config_file must be a non-empty str'
    assert validators.PanoptesValidators.valid_nonempty_string(config_spec_file), \
        u'config_spec_file must be a non empty str'

    try:
        config = ConfigObj(config_file, configspec=config_spec_file, interpolation=u'template', file_error=True)
    except IOError as e:
        raise PanoptesConfigurationParsingError(u'Error reading file: %s' % str(e))
    except ConfigObjError as e:
        raise PanoptesConfigurationParsingError(u'Error parsing config file "%s": %s' % (config_file, str(e)))

    validator = Validator()
    result = config.validate(validator, preserve_errors=True)

    if result is not True:
        errors = u''
        for (section_list, key, error) in flatten_errors(config, result):
            if key is None:
                errors += u'Section(s) ' + u','.join(section_list) + u' are missing\n'
            else:
                errors += u'The "' + key + u'" key in section "' + u','.join(section_list) + u'" failed validation\n'

        raise PanoptesConfigurationParsingError(u'Error parsing the configuration file: %s' % errors)

    return config


def convert_kv_str_to_dict(kv, prefix, kv_delimiter='|', prefix_delimiter='_'):
    """
    This function takes a string encoded list of key/value pairs, separated by a delimiter and turns it into a
    dictionary with key/value pairs with the given prefix and delimiter

    Examples:
        input: (value='metro|WA State|region|Western US|continent|North America|latitude|47.615|longitude|-122.339',
                kv_delimiter='|',
                prefix='geo',
                prefix_delimiter='_')

        output:  {'geo_metro': 'WA State',
                  'geo_region': 'Western US',
                  'geo_continent': 'North America',
                  'geo_latitude': '47.615',
                  'geo_longitude': '-122.339'}


    Args:
        kv (str): The string that encodes the key/value pairs
        prefix (str): The prefix to use for each key in the resultant dictionary
        kv_delimiter (str): The delimiter used to separate the keys and values in the encoded string
        prefix_delimiter (str): The delimiter to use between the prefix and key for the resultant dictionary


    Returns:
        dict: The parsed dictionary of keys and values. Will be an empty dictionary if there isn't a matching number
        of keys and values
    """
    assert validators.PanoptesValidators.valid_nonempty_string(kv), u'kv must be a non-empty string'
    assert validators.PanoptesValidators.valid_nonempty_string(prefix), u'prefix must be a non-empty string'
    assert validators.PanoptesValidators.valid_nonempty_string(kv_delimiter), u'kv_delimiter must be a non-empty string'
    assert validators.PanoptesValidators.valid_nonempty_string(prefix_delimiter), \
        u'prefix_delimiter must be a non-empty string'

    fields = kv.split(kv_delimiter)

    kvs = {}
    if (len(fields) % 2) == 0:
        kvs = {u"{}{}{}".format(prefix, prefix_delimiter, fields[x * 2]): fields[(x * 2) + 1] for x in
               range(0, old_div(len(fields), 2))}

    return kvs


def convert_celsius_to_fahrenheit(celsius):
    """
    Does exactly what the it says

    Args:
        celsius (int): The input to convert to fahrenheit

    Returns:
        int: The fahrenheit equivalent, rounded to two decimal places

    """

    return round(((celsius * 1.8) + 32), 2)


def ordered(obj):
    """
    Creates and ordered list from a given object. Can handle nested dictionaries and lists

    Args:
        obj (object): The object to order

    Returns:
        object: An ordered list or the original object if it is not a dict or list
    """
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in list(obj.items()))
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj


def transform_index_ipv6_address(ipv6_str):
    """
    Converts a substring of an SNMP index that contains an IPv6 address into a human readable format.
    Example:
        254.128.0.0.0.0.0.0.14.134.16.255.254.243.135.30 => fe80::e86:10ff:fef3:871e
    Args:
        ipv6_str (str): SNMP index substring containing an IPv6 address.
    Returns:
        str: human readable IPv6 address
    """
    parts = [u"{0:02x}".format(int(x)) for x in ipv6_str.split(u'.')]
    byte_string = u""
    for p, i in enumerate(parts):
        if p % 2 != 0:
            byte_string += u'{}{}:'.format(parts[p - 1].lstrip(u'0'), parts[p])

    result = str(byte_string[:-1])

    if isinstance(result, bytes):
        result = result.decode('utf-8')

    return str(ipaddress.ip_address(result))


def transform_octet_to_mac(octet_string):
    """
    Transforms SNMP Octet string to MAC address  separated by ':'
    Args:
        octet_string: SNMP Octet string

    Returns:
        MAC address separated by ':'
    """
    mac_address = ''

    if isinstance(octet_string, bytes):
        mac_address = u':'.join(u'{:02x}'.format(field) for field in octet_string)
    else:
        mac_address = u':'.join(u'{:02x}'.format(ord(field)) for field in octet_string)

    return mac_address.upper()


def transform_dotted_decimal_to_mac(dotted_decimal_mac):
    """
    Method to transform dotted decimals to Mac address
    Args:
        dotted_decimal_mac(string): dotted decimal string
    Returns:
        mac_address(string): Mac address separated by colon
    """

    decimal_mac = dotted_decimal_mac.split(u'.')
    hex_mac = u':'.join(str(hex(int(i)).lstrip(u'0x')).zfill(2) for i in decimal_mac).upper()
    return hex_mac


def convert_netmask_to_cidr(netmask):
    """
    Converts Netmask to cidr (255.255.255.0 => 24)
    Args:
        netmask (str): IP Netmask
    Returns:
        cidr (int): IP cidr
    """

    return sum([bin(int(x)).count(u"1") for x in netmask.split(u".")])
