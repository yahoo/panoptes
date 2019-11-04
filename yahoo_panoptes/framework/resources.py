"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.

This module defines resources and their related abstractions
"""
from builtins import next
from builtins import str
from builtins import range
from builtins import object
import hashlib
import json
import re
import sqlite3
import threading
from collections import OrderedDict
from time import time

from cached_property import threaded_cached_property
from pyparsing import CaselessKeyword, Group, Word, oneOf, delimitedList, ZeroOrMore, Forward, \
    ParseException, Optional, QuotedString, CaselessLiteral, Literal, alphanums, ParseResults, upcaseTokens, \
    downcaseTokens
from six import string_types, integer_types

from yahoo_panoptes.framework import const
from yahoo_panoptes.framework.context import PanoptesContext
from yahoo_panoptes.framework.exceptions import PanoptesBaseException
from yahoo_panoptes.framework.utilities.key_value_store import PanoptesKeyValueStore
from yahoo_panoptes.framework.validators import PanoptesValidators


class PanoptesResourceError(PanoptesBaseException):
    """
    The base class for all Panoptes Resources errors
    """
    pass


class PanoptesResourceCacheException(PanoptesResourceError):
    """
    The base class for PanoptesResourceCache errors
    """
    pass


class PanoptesResourceValidators(object):
    @classmethod
    def valid_panoptes_resource(cls, resource):
        """
        Checks if the passed object is an instance of PanoptesResource

        Args:
            resource (object): The object to check

        Returns:
            bool: True if the object is not null and is an instance of PanoptesResource
        """
        return resource and isinstance(resource, PanoptesResource)


class PanoptesResource(object):
    """
    Representation of the a device/endpoint that should be monitored

    A resource in Panoptes is uniquely defined by it's class, subclass, type and id. Class, subclass and type jointly \
    act as the namespace of the resource - the id must be unique within this namespace

    Args:
        resource_site (str): The name of the site to which the resource belongs (e.g. colo name)
        resource_class (str): The class of the resource (e.g. 'network')
        resource_subclass (str): The subclass of the resource (e.g. 'load-balancer')
        resource_type (str): The type of the resource (e.g. 'a10')
        resource_id (str): A unique identifier for the resource
        resource_endpoint (str): The endpoint that should be polled by the monitoring system for the resource
        resource_creation_timestamp (float): A  UTC Unix Epoch timestamp of when the Resource was created. If this is
        not provided, then the UTC timestamp of the current time is automatically added
        resource_plugin (str): The name of the discovery plugin that found this resource
        resource_ttl(int): Resource cache ttl in seconds

    Notes:
        - All the parameters (except resource_endpoint and resource_creation_timestamp) are
        required and cannot be null or empty
        - The class, subclass, type and id are arbitrary strings and not validated
        - Please do not use randomly generated UUIDs/GUIDs for the id of resource - the reason is that you create
        another resource with exactly the same data everywhere except the id, Panoptes  will treat these as
        two separate resources throughout the system. If you plan to use UUIDs/GUIDs, please make sure that they come
        from a central source of truth/registry and that the same resource is assigned the same UUID/GUID every time
        the resource object is created
    """

    _metadata_key = re.compile(r"^[^\d\W]\w*\Z")

    def __init__(self, resource_site, resource_class, resource_subclass, resource_type,
                 resource_id, resource_endpoint, resource_creation_timestamp=None,
                 resource_plugin=None, resource_ttl=const.RESOURCE_MANAGER_RESOURCE_EXPIRE):
        assert PanoptesValidators.valid_nonempty_string(resource_site), u'resource_site must be a non-empty str'
        assert PanoptesValidators.valid_nonempty_string(resource_class), u'resource_class must be a non-empty str'
        assert PanoptesValidators.valid_nonempty_string(resource_subclass), u'resource_subclass must be a non-empty'
        assert PanoptesValidators.valid_nonempty_string(resource_type), u'resource_type must be a non-empty str'
        assert PanoptesValidators.valid_nonempty_string(resource_id), u'resource_id must be a non-empty str'
        assert PanoptesValidators.valid_nonempty_string(resource_endpoint), u'resource_endpoint must be a non-empty str'
        assert PanoptesValidators.valid_none_or_nonempty_string(
            resource_plugin), u'resource_plugin must be None or a non-empty str'
        assert PanoptesValidators.valid_nonzero_integer(resource_ttl), u'resource_ttl must be an integer greater than 0'

        self.__data = OrderedDict()
        self.__data[u'resource_site'] = str(resource_site)
        self.__data[u'resource_class'] = str(resource_class)
        self.__data[u'resource_subclass'] = str(resource_subclass)
        self.__data[u'resource_type'] = str(resource_type)
        self.__data[u'resource_id'] = str(resource_id)
        self.__data[u'resource_endpoint'] = str(resource_endpoint)
        self.__data[u'resource_metadata'] = OrderedDict()
        if not resource_creation_timestamp:
            self.__data[u'resource_creation_timestamp'] = time()
        else:
            self.__data[u'resource_creation_timestamp'] = resource_creation_timestamp
        self.__data[u'resource_plugin'] = resource_plugin
        self.__data[u'resource_metadata'][u'_resource_ttl'] = str(resource_ttl)

    def add_metadata(self, key, value):
        """
        Add a metadata key/value pair

        Args:
            key(str): The syntax for a key is the same as for valid Python identifier: (letter|"_") (letter | digit "_")
            value(str): A pipe (|) is a special reserved character and cannot be present in a value

        Returns:
            None

        Raises:
            ValueError: If the key or value don't match the acceptable string patterns, a ValueError is raised

        Examples:
            add_metadata('os_name', 'Advanced Core OS')
            add_metadata('os_version', '2.6.1-GR1-P16')
        """
        assert PanoptesValidators.valid_nonempty_string(key), u'key must be a non-empty str or unicode'
        assert PanoptesValidators.valid_nonempty_string(value), u'value must be a non-empty str or unicode'

        if not self.__class__._metadata_key.match(key):
            raise ValueError(u'metadata key "%s" has to match pattern: (letter|"_") (letter | digit | "_")*' % key)

        if u'|' in value:
            raise ValueError(u'metadata value "%s" cannot contain |' % value)

        self.__data[u'resource_metadata'][key] = value

    @property
    def resource_site(self):
        """
        The site name to which the resource belongs

        Returns:
            str

        """
        return self.__data[u'resource_site']

    @property
    def resource_class(self):
        """
        The class of the resource

        Returns:
            str

        """
        return self.__data[u'resource_class']

    @property
    def resource_subclass(self):
        """
        The subclass of the resources

        Returns:
            str

        """
        return self.__data[u'resource_subclass']

    @property
    def resource_type(self):
        """
        The type of the resource

        Returns:
            str

        """
        return self.__data[u'resource_type']

    @property
    def resource_id(self):
        """
        The id of the resource

        Returns:
            str

        """
        return self.__data[u'resource_id']

    @property
    def resource_endpoint(self):
        """
        The endpoint to be monitored for the resource

        Returns:
            str

        """
        return self.__data[u'resource_endpoint']

    @property
    def resource_ttl(self):
        """
        The cache ttl seconds of the resource

        Returns:
            str

        """
        return self.__data[u'resource_metadata'][u'_resource_ttl']

    @property
    def resource_creation_timestamp(self):
        return self.__data[u'resource_creation_timestamp']

    @property
    def resource_plugin(self):
        return self.__data[u'resource_plugin']

    @threaded_cached_property
    def serialization_key(self):
        key = const.KV_STORE_DELIMITER.join([
            u'plugin', self.resource_plugin,
            u'site', self.resource_site,
            u'class', self.resource_class,
            u'subclass', self.resource_subclass,
            u'type', self.resource_type,
            u'id', self.resource_id,
            u'endpoint', self.resource_endpoint
        ])

        return key

    @property
    def resource_metadata(self):
        """
        The metadata associated with the resource

        Returns:
            dict

        """
        return self.__data[u'resource_metadata']

    @property
    def json(self):
        """
        The JSON representation of the resource

        Returns:
            str: The JSON representation of the resource
        """
        return json.dumps(self.__data)

    @property
    def raw(self):
        return self.__data

    def __repr__(self):
        return str(self.serialization_key)

    def __hash__(self):
        return int(hashlib.md5(self.serialization_key.encode('utf-8')).hexdigest(), 16)

    def __lt__(self, other):
        if not PanoptesResourceValidators.valid_panoptes_resource(other):
            return False
        return self.resource_id < other.resource_id

    def __eq__(self, other):
        if not PanoptesResourceValidators.valid_panoptes_resource(other):
            return False
        return (
            self.resource_site == other.resource_site and
            self.resource_class == other.resource_class and
            self.resource_subclass == other.resource_subclass and
            self.resource_type == other.resource_type and self.resource_id == other.resource_id
        )

    @staticmethod
    def resource_from_dict(resource_dict):
        assert isinstance(resource_dict, dict), u'resource_dict must be a dict'

        resource = PanoptesResource(resource_plugin=resource_dict[u'resource_plugin'],
                                    resource_site=resource_dict[u'resource_site'],
                                    resource_class=resource_dict[u'resource_class'],
                                    resource_subclass=resource_dict[u'resource_subclass'],
                                    resource_type=resource_dict[u'resource_type'],
                                    resource_id=resource_dict[u'resource_id'],
                                    resource_endpoint=resource_dict[u'resource_endpoint'],
                                    resource_ttl=int(resource_dict[u'resource_metadata'].get(
                                        u'_resource_ttl', const.RESOURCE_MANAGER_RESOURCE_EXPIRE)),
                                    resource_creation_timestamp=resource_dict[u'resource_creation_timestamp'], )

        for metadata_key in resource_dict[u'resource_metadata']:
            resource.add_metadata(metadata_key, resource_dict[u'resource_metadata'][metadata_key])

        return resource


class PanoptesResourceSet(object):
    """
    An (un-ordered) set of PanoptesResources
    """

    def __init__(self):
        self.__data = dict()
        self.__data[u'resources'] = set()
        self.__data[u'resource_set_creation_timestamp'] = time()
        self.__data[u'resource_set_schema_version'] = u'0.1'

    def add(self, resource):
        """
        Add a resource to the set

        Args:
            resource (PanoptesResource): The resource to add

        Returns:
            None

        """
        assert PanoptesResourceValidators.valid_panoptes_resource(resource), \
            u'resource must an instance of PanoptesResource'
        self.__data[u'resources'].add(resource)

    def remove(self, resource):
        """
        Remove a resource from the set

        Args:
            resource (PanoptesResource): The resource to remove

        Returns:
            None

        """
        assert PanoptesResourceValidators.valid_panoptes_resource(resource), \
            u'resource must an instance of PanoptesResource'
        self.__data[u'resources'].remove(resource)

    def get_resources_by_site(self):
        resources_by_site = dict()
        for resource in self.resources:
            if not resources_by_site.get(resource.resource_site, None):
                resources_by_site[resource.resource_site] = list()

            resources_by_site[resource.resource_site].append(resource)

        return resources_by_site

    @property
    def resources(self):
        """
        Return the list of resources in this set

        Returns:
            list: The set of resources

        """
        return self.__data[u'resources']

    @property
    def resource_set_creation_timestamp(self):
        return self.__data[u'resource_set_creation_timestamp']

    @resource_set_creation_timestamp.setter
    def resource_set_creation_timestamp(self, timestamp):
        """
            Sets the timestamp of the

        Args:
            timestamp (int, float): The new timestamp

        Returns:
            None

        Raises:
            AssertionError: If the passed timestamp is not an int or float or is too old or too much in the future, \
            an AssertionError would be raised

        """
        assert PanoptesValidators.valid_timestamp(timestamp), u'timestamp should be an Unix epoch int|float ' \
                                                              u'not more than 7 days old or more than 60 ' \
                                                              u'seconds in the future'
        self.__data[u'resource_set_creation_timestamp'] = timestamp

    @property
    def resource_set_schema_version(self):
        return self.__data[u'resource_set_schema_version']

    def __iter__(self):
        return iter(self.__data[u'resources'])

    def __next__(self):
        """
        Returns the next resource in the set

        Returns:
            PanoptesResource: The next resource in the set
        """
        return next(iter(self.__data[u'resources']))

    @property
    def json(self):
        """
        The JSON representation of the resource set

        Returns:
            str: The JSON representation of the resource set
        """
        return json.dumps(self.__data, cls=PanoptesResourceEncoder)

    def __repr__(self):
        return str(self.__data[u'resources'])

    def __len__(self):
        return len(self.__data[u'resources'])


class PanoptesResourcesKeyValueStore(PanoptesKeyValueStore):
    """
    A custom Key/Value store for Panoptes resources
    """
    redis_group = const.RESOURCE_MANAGER_REDIS_GROUP

    def __init__(self, panoptes_context):
        super(PanoptesResourcesKeyValueStore, self).__init__(panoptes_context,
                                                             const.RESOURCE_MANAGER_KEY_VALUE_NAMESPACE)


class PanoptesResourceStore(object):
    """
    This class implements methods to fetch resources from Redis and create in-memory objects of the same
    """
    _regex_key = re.compile(
            r'(^plugin\|)(?P<plugin>.*?)(\|site\|)(?P<site>.*?)(\|class\|)(?P<class>.*?)(\|subclass\|)('
            r'?P<subclass>.*?)(\|type\|)(?P<type>.*?)(\|id\|)(?P<id>.*?)(\|endpoint\|)(?P<endpoint>.*?$)')
    _regex_timestamp = re.compile(r'timestamp\|(?P<timestamp>(\d{10}\.\d{1,2}))\|?(?P<meta>.*)$')
    _regex_meta = re.compile(r'(meta\.)(.*?)(\|)(.*?)(\||$)')

    def __init__(self, panoptes_context):
        self.__panoptes_context = panoptes_context
        try:
            self.__kv = self.__panoptes_context.get_kv_store(PanoptesResourcesKeyValueStore)
        except Exception as e:
            raise e

    def get_resources(self, site=None, plugin_name=None):
        """
        Get all resources from the Redis store

        The serialization that this expects in Redis is:

        key: 'resource:resource_site:<sitename>:resource_class:<class>:resource_subclass:<subclass>:resouce_type
        :<type>:resource_id:<id>:resource_endpoint:endpoint'
        value: 'timestamp:<unix epoch>{:meta:<key name>:<value>}*'

        Args:
            site (str, None): An optional string which filters resources by site
            plugin_name (str, None): An optional string which filters resources by the plugin they were discovered
                                     through

        Returns:
            PanoptesResourceSet: All resources fetched from the Redis store
        """
        assert PanoptesValidators.valid_none_or_string(site), u'site should be None or a str'
        assert PanoptesValidators.valid_none_or_string(plugin_name), u'plugin_name should be None or a str'

        logger = self.__panoptes_context.logger
        resources = PanoptesResourceSet()

        key_namespace = u''

        if plugin_name:
            key_namespace += u'plugin|' + plugin_name + u'|'

        if site:
            key_namespace += u'site|' + site + u'|'

        key_namespace += u'*'

        logger.info(u'Trying to get all resources under key namespace "%s"' % key_namespace)

        start = time()
        for key in self.__kv.find_keys(pattern=key_namespace):
            logger.debug(u'Attempting to get resource under key "%s"' % key)

            try:
                resource = self.get_resource(key)
                logger.debug(u'Found resource "%s"' % resource)
            except:
                logger.exception(u'Error trying to get "%s", skipping resource' % key)
                continue

            resources.add(resource)

        end = time()
        logger.info(u'Fetched %d resource(s) in %.2f seconds' % (len(resources), end - start))

        return resources

    def get_resource(self, resource_key):
        assert PanoptesValidators.valid_nonempty_string(resource_key), u'resource_key must be a non-empty str or' \
                                                                       u' unicode'

        logger = self.__panoptes_context.logger

        try:
            value = self.__kv.get(resource_key)
        except Exception as e:
            logger.exception(u'Error trying to get value from key-value store for key "%s"' % resource_key)
            raise PanoptesResourceError(u'Error trying to fetch value for key "%s": %s ' % (resource_key, repr(e)))

        if not value:
            logger.exception(u'Error -- No resource found for key "%s"' % resource_key)
            raise PanoptesResourceError(u'No resource found for key "%s"' % resource_key)

        return self._deserialize_resource(resource_key, value)

    def add_resource(self, plugin_signature, resource):
        assert PanoptesValidators.valid_nonempty_string(plugin_signature), u'plugin_signature must be a non-empty str'
        assert PanoptesResourceValidators.valid_panoptes_resource(resource), u'resource must be a non-empty instance ' \
                                                                             u'of PanoptesResource'
        key, value = self._serialize_resource(resource)

        try:
            self.__kv.set(key, value, expire=int(resource.resource_ttl))
        except Exception as e:
            raise PanoptesResourceError(u'Error trying to add resource "%s": %s' % (resource, str(e)))

    def delete_resource(self, plugin_signature, resource):
        assert PanoptesValidators.valid_nonempty_string(plugin_signature), u'plugin_signature must be a non-empty str'
        assert PanoptesResourceValidators.valid_panoptes_resource(resource), u'resource must be a non-empty instance ' \
                                                                             u'of PanoptesResource'

        key, value = self._serialize_resource(resource)

        try:
            self.__kv.delete(key)
        except Exception as e:
            raise PanoptesResourceError(u'Error trying to delete resource "%s": %s' % (resource, str(e)))

    @staticmethod
    def _serialize_resource(resource):
        assert PanoptesResourceValidators.valid_panoptes_resource(resource), u'resource must be a non-empty instance ' \
                                                                             u'of PanoptesResource'
        key = resource.serialization_key

        value = u'timestamp|' + str(resource.resource_creation_timestamp) + u'|'

        for metadata_key in resource.resource_metadata:
            value += u'meta.' + metadata_key + u'|' + resource.resource_metadata[metadata_key] + u'|'

        return key, value

    def _deserialize_resource(self, key, value):
        """
        This method parses the key/value pair to create a PanoptesResource

        Args:
            key (str): The serialized key
            value (str): The serialized value

        Returns:
            PanoptesResource: A PanoptesResource object which represented by the serialized key and value
        """
        parsed_key = self.__class__._regex_key.search(key)

        if not parsed_key:
            raise PanoptesResourceError(u'Resource key "%s" does not match pattern' % key)

        parsed_value = self.__class__._regex_timestamp.search(value)

        if not parsed_value:
            raise PanoptesResourceError(u'Value for resource "%s" does not match pattern' % value)

        meta = self.__class__._regex_meta.findall(parsed_value.group(u'meta'))
        resource_metadata = dict()
        for m in meta:
            resource_metadata[m[1]] = m[3]

        resource = PanoptesResource(resource_site=parsed_key.group(u'site'),
                                    resource_class=parsed_key.group(u'class'),
                                    resource_subclass=parsed_key.group(u'subclass'),
                                    resource_type=parsed_key.group(u'type'),
                                    resource_id=parsed_key.group(u'id'),
                                    resource_endpoint=parsed_key.group(u'endpoint'),
                                    resource_plugin=parsed_key.group(u'plugin'),
                                    resource_ttl=int(resource_metadata.get(u'_resource_ttl',
                                                                           const.RESOURCE_MANAGER_RESOURCE_EXPIRE)),
                                    resource_creation_timestamp=float(parsed_value.group(u'timestamp')))

        for k in resource_metadata:
            resource.add_metadata(k, resource_metadata[k])

        return resource


class PanoptesResourceDSL(object):
    """
    The class implements a SQL WHERE clause like resource selection/filtering DSL

    The following operators are supported: =, !=, eq, ne, LIKE, AND, OR, NOT, IN

    Please see the 'Examples' and 'Notes' for usage and constraints

    Args:
        query(str): The query to select/filter resources
        panoptes_context(PanoptesContext): The PanoptesContext associated with the calling process

    Returns:
        PanoptesResourceDSL

    Examples:
        resource_site = "dc1"
        resource_site = "dc1" AND resource_class = "network"
        resource_metadata.os_version LIKE "4%" OR resource_site NOT IN ("dc1", "dc2")

    Notes:
        * All the values in the resource selection are treated as strings and should be quoted with double quotes
        * NOT operator can be used in front of LIKE and IN:
            * resource_site NOT IN ("dc1", "dc2")
            * resource_site NOT LIKE 'dc%'
        * Grouping by parenthesis 'e.g. NOT (resource_site = "dc1" OR resource_site = "dc2")' is currently not supported
    """

    def __init__(self, query, panoptes_context):
        assert PanoptesValidators.valid_nonempty_string(query), u'query must be an instance of str'
        assert panoptes_context and isinstance(panoptes_context,
                                               PanoptesContext), u'panoptes_context must be an instance of ' \
                                                                 u'PanoptesContext'
        self._query = query
        logger = panoptes_context.logger

        logger.info(u'Parsing query expression: %s' % query)
        try:
            tokens = self._parse_query()
        except ParseException as e:
            logger.error(u'Error in parsing expression "%s": %s' % (query, str(e)))
            raise e
        else:
            logger.debug(u'Tokens = %s' % tokens)
            self._tokens = tokens

    def _parse_query(self):
        """
        Defines and parses the Resource DSL based on the query associated with the PanoptesResourceDSL object

        Returns:
            list: The list of tokens parsed

        Raises:
            ParseException: This exception is raised if any parsing error occurs
        """
        resource_fields = oneOf(
                u'resource_site resource_class resource_subclass resource_type resource_id resource_endpoint',
                caseless=True)
        resource_metadata = CaselessLiteral(u'resource_metadata') + Literal(u'.') + Word(alphanums + u'_')

        and_ = CaselessKeyword(u'AND').setParseAction(upcaseTokens)
        or_ = CaselessKeyword(u'OR').setParseAction(upcaseTokens)
        not_ = CaselessKeyword(u'NOT').setParseAction(upcaseTokens)
        in_ = CaselessKeyword(u'IN').setParseAction(upcaseTokens)
        like_ = CaselessKeyword(u'LIKE').setParseAction(upcaseTokens)

        operators = oneOf(u"= != eq ne", caseless=True).setParseAction(upcaseTokens)

        query_expression = Forward()

        query_l_val = (resource_fields | resource_metadata).setParseAction(downcaseTokens)
        query_r_val = QuotedString(quoteChar=u'"', escChar=u'\\')

        query_condition = Group(
                (query_l_val + operators + query_r_val) |
                (query_l_val + Optional(not_) + like_ + query_r_val) |
                (query_l_val + Optional(not_) + in_ + u'(' + delimitedList(query_r_val) + u')')
        )

        query_expression << query_condition - ZeroOrMore((and_ | or_) - query_condition)

        try:
            tokens = query_expression.parseString(self._query, parseAll=True)
        except ParseException as e:
            raise e

        return tokens

    @property
    def tokens(self):
        """
        Return the tokens parsed from the query

        Returns:
            pyparsing.Parser: The tokens parsed from the query

        """
        return self._tokens

    @property
    def sql(self):
        """
        The property returns a part of the WHERE clause based on the input query which is compatible with SQLite

        Returns:
            str: A partial WHERE clause

        """
        metadata_sql = u''
        intersect_sql_clause = u''
        union_sql_clause = u''
        sql = u''
        metadata_first_clause = True

        for i in range(0, len(self._tokens)):
            token = self._tokens[i]
            if isinstance(token, ParseResults):
                if token[0] == u'resource_metadata':
                    metadata_sql_clause = u'('
                    metadata_sql_clause += (u'resource_metadata.key = ' + u'"' + token[2] + u'"' +
                                            u' AND resource_metadata.value ')
                    if token[3] == u'NOT':
                        metadata_sql_clause += token[3] + u' ' + token[4] + u' ' + self._process_rval(token[5:])
                    else:
                        metadata_sql_clause += token[3] + u' ' + self._process_rval(token[4:])
                    metadata_sql_clause += u')'
                    if metadata_first_clause:
                        metadata_first_clause = False
                        metadata_sql += metadata_sql_clause
                    else:
                        if i > 0:
                            partial_sql_clause = (
                                u'SELECT resource_metadata.id FROM resources,resource_metadata WHERE ' +
                                metadata_sql_clause + u' AND resource_metadata.id = resources.id ')
                            if self._tokens[i - 1] == u'AND':
                                intersect_sql_clause += u'INTERSECT ' + partial_sql_clause
                            elif self._tokens[i - 1] == u'OR':
                                union_sql_clause += u'UNION ' + partial_sql_clause
                else:
                    sql += u'resources.' + token[0] + u' '
                    if token[1] == u'NOT':
                        sql += token[1] + u' ' + token[2] + u' ' + self._process_rval(token[3:])
                    else:
                        sql += token[1] + u' ' + self._process_rval(token[2:])
            else:
                if isinstance(self._tokens[i + 1], ParseResults) and self._tokens[i + 1][0] != u'resource_metadata':
                    sql += u' ' + token + u' '

        if metadata_sql:
            sql += u' AND (' + metadata_sql + u')'
            where_clause = (u"(SELECT resource_metadata.id FROM resources,resource_metadata " +
                            u"WHERE (" + sql + u" AND resource_metadata.id = resources.id) " +
                            union_sql_clause +
                            intersect_sql_clause +
                            u"GROUP BY resource_metadata.id " +
                            u"ORDER BY resource_metadata.id" +
                            u") AS filtered_resources")

            final_sql = (u'SELECT resources.*,group_concat(key,"|"),group_concat(value,"|") ' +
                         u'FROM ' + where_clause + u', resources, resource_metadata ' +
                         u'WHERE resources.id = filtered_resources.id ' +
                         u'AND resource_metadata.id = filtered_resources.id ' +
                         u'GROUP BY resource_metadata.id')
        else:
            final_sql = (u'SELECT resources.*, group_concat(key,"|"), group_concat(value,"|") ' +
                         u'FROM resources ' +
                         u'LEFT JOIN resource_metadata ON resources.id = resource_metadata.id ' +
                         u"WHERE (" + sql + u") " +
                         union_sql_clause +
                         intersect_sql_clause +
                         u"GROUP BY resource_metadata.id " +
                         u"ORDER BY resource_metadata.id")
        return final_sql

    @staticmethod
    def _process_rval(rval):
        """
        This method creates SQL for the right hand side of an expression

        Args:
            rval (list):

        Returns:
            str: The partial SQL statement
        """
        sql = u''
        if rval[0] == u'(':
            sql += u'(' + u','.join([u'"' + x + u'"' for x in rval[1:len(rval) - 1]]) + u')'
        else:
            sql = u'"' + rval[0] + u'"'

        return sql


class PanoptesResourceCache(object):
    """
    This class implements an in-memory cache of PanoptesResources which can be queried using the DSL

    Internally, it fetches all Panoptes Resources from Redis and creates an in-memory SQLite DB which can be queried

    Args:
        panoptes_context (PanoptesContext): The Panoptes Context associated with the calling Plugin Scheduler
    """

    def __init__(self, panoptes_context):
        self._resources = None
        self._db = None
        self._cursor = None
        self._panoptes_context = panoptes_context
        self._resources_store = None
        self._cached_resources = dict()
        self._lock = threading.Lock()

    def get_resources(self, query):
        """
        Returns a resource set filtered on the provided Resource DSL query

        Args:
            query (str): The Panoptes Resource DSL filter to apply

        Returns:
            PanoptesResourceSet: The resource set containing the filtered resources
        """
        logger = self._panoptes_context.logger

        logger.info(u'Going to query with resource filter "%s"' % query)
        resource_filter = PanoptesResourceDSL(query, self._panoptes_context)
        logger.debug(u'SQL for resource filter "%s": %s' % (query, resource_filter.sql))

        if resource_filter.sql in self._cached_resources:
            logger.debug(u'Returning cached resources')
            return self._cached_resources[resource_filter.sql]

        try:
            self._cursor.execute(resource_filter.sql)
        except Exception as e:
            raise PanoptesResourceCacheException(
                    u'Error trying to execute resource filter SQL "%s": %s' % (resource_filter.sql, str(e)))

        resource_set = PanoptesResourceSet()
        rows = self._cursor.fetchall()

        for row in rows:
            logger.debug(u'For resource filter "%s", found resource %s' % (query, row))
            resource = PanoptesResource(resource_site=row[1], resource_class=row[2], resource_subclass=row[3],
                                        resource_type=row[4], resource_id=row[5], resource_endpoint=row[6],
                                        resource_plugin=row[7])

            try:
                # Columns 8 & 9 contain the metadata keys and values respectively
                if row[8] and row[9]:
                    metadata_keys = row[8].split(u'|')
                    metadata_values = row[9].split(u'|')
                    for i in range(len(metadata_keys)):
                        resource.add_metadata(metadata_keys[i], metadata_values[i])
            except Exception as e:
                logger.error(
                    u'Either resource metadata key or value are not correct, skipping resource "%s": %s' % (
                        row, str(e)))
                continue

            resource_set.add(resource)

        with self._lock:
            self._cached_resources[resource_filter.sql] = resource_set
        logger.info(u'For resource filter "%s", found %d resources' % (query, len(resource_set)))
        logger.debug(u'For resource filter "%s", returning resource set: %s' % (query, resource_set))
        return resource_set

    def setup_resource_cache(self):
        """
        Gets resources from a PanoptesResourceStore (Redis) and populates the in-memory SQLite DB

        Returns:
            None

        Raises:
            Exception: Passes through any exceptions that happen during fetching or population of resources
        """
        logger = self._panoptes_context.logger

        try:
            self._resources_store = PanoptesResourceStore(self._panoptes_context)
        except Exception as e:
            logger.error(u'Error while setting up PanoptesResourceStore: %s' % repr(e))
            raise PanoptesResourceCacheException(u"Error while setting up resources_store")

        try:
            self._create_db()
        except Exception as e:
            logger.error(u'Error while setting up the in-memory SQLite DB: %s' % repr(e))
            raise PanoptesResourceCacheException(u"Error while setting up the in-memory SQLite DB")

        logger.info(u'Attempting to get all resources')
        try:
            self._resources = self._resources_store.get_resources()
        except Exception as e:
            logger.error(u'Error while getting resources from PanoptesResourceStore: %s' % repr(e))
            raise PanoptesResourceCacheException(u"Error while getting resources from PanoptesResourceStore")

        logger.info(u'Got %d resources' % len(self._resources))
        logger.debug(u'Resources: %s' % self._resources)

        for resource in self._resources:
            self._cursor.execute(
                    u'''
                    INSERT INTO resources(resource_site,
                                          resource_class,
                                          resource_subclass,
                                          resource_type,
                                          resource_id,
                                          resource_endpoint,
                                          resource_plugin)
                    VALUES (?,?,?,?,?,?,?)
                    ''''',
                    (resource.resource_site,
                     resource.resource_class,
                     resource.resource_subclass,
                     resource.resource_type,
                     resource.resource_id,
                     resource.resource_endpoint,
                     resource.resource_plugin)
            )

            rowid = self._cursor.lastrowid
            logger.debug(u'Inserted row into database with rowid %d' % rowid)
            logger.debug(u'Going to update metadata associated with the resource')

            for key in resource.resource_metadata:

                self._cursor.execute(
                        u'''
                        INSERT INTO resource_metadata(id, key, value) VALUES(?,?,?)
                        ''',
                        (rowid, key, resource.resource_metadata[key])
                )
        self._db.commit()
        # Invalidate cached resources
        with self._lock:
            self._cached_resources = dict()

        logger.info(u"Created DB")

    def _create_db(self):
        """
        Creates the in-memory SQLite DB and stores reference to the cursor

        Returns:
            None
        """
        logger = self._panoptes_context.logger

        logger.info(u'Creating in-memory SQLite DB')
        self._db = sqlite3.connect(u':memory:')
        self._cursor = self._db.cursor()
        self._cursor.arraysize = const.RESOURCE_CACHE_DB_CURSOR_SIZE

        logger.info(u'Creating tables in SQLite DB')
        self._cursor.execute(
                u'''
                CREATE TABLE resources
                (id INTEGER PRIMARY KEY AUTOINCREMENT,
                resource_site VARCHAR(255) NOT NULL,
                resource_class VARCHAR(255) NOT NULL,
                resource_subclass VARCHAR(255) NOT NULL,
                resource_type VARCHAR(255) NOT NULL,
                resource_id VARCHAR(255) NOT NULL,
                resource_endpoint VARCHAR(255) NOT NULL,
                resource_plugin VARCHAR(255) NOT NULL
                )
                '''
        )

        self._cursor.execute(
                u'''
                CREATE TABLE resource_metadata
                (id INTEGER,
                key VARCHAR(255) NOT NULL,
                value VARCHAR(255) NOT NULL,
                FOREIGN KEY(id) REFERENCES resources(id)
                )
                '''
        )

        self._cursor.execute(u'CREATE INDEX index_resource_site on resources (resource_site);')
        self._cursor.execute(u'CREATE INDEX index_resource_class on resources (resource_class);')
        self._cursor.execute(u'CREATE INDEX index_resource_subclass on resources (resource_subclass);')
        self._cursor.execute(u'CREATE INDEX index_resource_type on resources (resource_type);')
        self._cursor.execute(u'CREATE INDEX index_resource_id on resources (resource_id);')
        self._cursor.execute(u'CREATE INDEX index_resource_endpoint on resources (resource_endpoint);')
        self._cursor.execute(u'CREATE INDEX index_resource_plugin on resources (resource_plugin);')
        self._cursor.execute(u'CREATE INDEX index_metadata_id on resource_metadata (id);')
        self._cursor.execute(u'CREATE INDEX index_metadata_key on resource_metadata (key);')
        self._cursor.execute(u'CREATE INDEX index_metadata_value on resource_metadata (value);')

        self._db.commit()

        logger.info(u'Finished creating tables in SQLite DB')

    def close_resource_cache(self):
        """
        Closes the connection to the in-memory SQLite DB

        Returns:
            None
        """
        if self._db:
            self._db.close()
        else:
            self._panoptes_context.logger.error(u"Attempted to close connection to SQLite DB that was not open")


class PanoptesResourceEncoder(json.JSONEncoder):
    # https://github.com/PyCQA/pylint/issues/414
    def default(self, o):  # pylint: disable=E0202
        if isinstance(o, set):
            return list(o)
        if isinstance(o, PanoptesResource):
            return o.__dict__[u'_PanoptesResource__data']
        return json.JSONEncoder.default(self, o)
