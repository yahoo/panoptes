"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""

from .. import const
from ..plugins.panoptes_base_plugin import PanoptesPluginInfo


def expires(plugin):
    """
    Returns the number of seconds after which to expire a pending plugin task

    Args:
        plugin (PanoptesPluginInfo): The plugin for which to return the expiry time

    Returns:
        int: The number of seconds after which to consider the task expired
    """
    assert isinstance(plugin, PanoptesPluginInfo), 'plugin must be an instance of PanoptesPluginInfo'

    return round(plugin.execute_frequency * const.PLUGIN_AGENT_PLUGIN_EXPIRES_MULTIPLE)


def time_limit(plugin):
    """
    Returns the number of seconds after which to stop a running plugin task

    Args:
        plugin (PanoptesPluginInfo): The plugin for which to return the time limit

    Returns:
        int: The number of seconds after which to stop a running plugin task
    """
    assert isinstance(plugin, PanoptesPluginInfo), 'plugin must be an instance of PanoptesPluginInfo'

    return round(plugin.execute_frequency * const.PLUGIN_AGENT_PLUGIN_TIME_LIMIT_MULTIPLE)
