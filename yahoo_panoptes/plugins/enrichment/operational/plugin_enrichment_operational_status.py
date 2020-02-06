from __future__ import division
from past.utils import old_div
import datetime
import time

from sysdescrparser import sysdescrparser

from yahoo_panoptes.enrichment.enrichment_plugin import PanoptesEnrichmentPlugin
from yahoo_panoptes.enrichment.schema.operational import PanoptesOperationalEnrichmentGroup
from yahoo_panoptes.framework.enrichment import PanoptesEnrichmentSet, PanoptesEnrichmentGroupSet
from yahoo_panoptes.plugins.helpers.snmp_connections import PanoptesSNMPConnectionFactory
from yahoo_panoptes.plugins.polling.utilities.polling_status import PanoptesPollingStatus


class OperationalEnrichment(PanoptesEnrichmentPlugin):
    def __init__(self):

        self.OPERATIONAL_ENRICHMENTS = {
            u'snmpenginetime': self._callback_snmpenginetime,
            u'last_updated': self._callback_last_updated,
        }

        self._sysdescr_text = None
        self._sysdescr = {}

        self._plugin_context = None
        self._conf = None
        self._logger = None

        self._enrichment_ttl = None
        self._execute_frequency = None
        self._snmp_connection = None

        self._device = None
        self._device_name = None

        self._operational_enrichment_group = None
        self._operational_enrichment_group_set = None

        self._polling_status = None

        super(OperationalEnrichment, self).__init__()

    @staticmethod
    def timeticks_to_seconds(timeticks):
        """
        Timeticks are 1/100ths of a second.

        Args:
            timeticks (float):

        Returns:
            int: seconds
        """
        return int(old_div(timeticks, 100))

    def _callback_snmpenginetime(self):
        """
        Just grabs the snmpenginetime, which is ubiquitous and in seconds from the last restart.

        Returns:
            int: seconds since the last restart
        """
        oid = u'.1.3.6.1.6.3.10.2.1.3.0'
        return int(self._get_oid_result(oid))

    def _get_oid_result(self, oid):
        """
        Naive return of an snmp metric endpoint
        Args:
            oid (str): OID to pull

        Returns:
            str: unicode value returned
        """
        try:
            result = self._snmp_connection.get(oid)
        except Exception as e:
            raise e

        return result.value

    def _callback_last_updated(self):
        """See subclass"""
        pass

    def _get_sysuptime(self):
        """
        Ubiquitous:  32bit integer in 1/100s of a second.  Rolls over after 496 days.  snmpengine time is better, but
        I'm leaving this here until I know we don't need it anymore.

        Returns:
            int: seconds since the last restart
        """
        oid = u'.1.3.6.1.2.1.1.3.0'
        timeticks = int(self._get_oid_result(oid))
        seconds = self.timeticks_to_seconds(timeticks)
        return seconds

    def _get_sysdescr(self):
        """
        Query devices for system model, vendor, os version

        Returns:
            str: system description string
        """
        oid = u'.1.3.6.1.2.1.1.1.0'
        descr = self._get_oid_result(oid)

        if isinstance(descr, bytes):
            descr = descr.decode('utf-8')
        else:
            descr = str(descr)

        return descr if descr else u''

    @staticmethod
    def seconds_to_string(seconds):
        """
        Making the seconds epoch more readable
        Args:
            seconds (int):

        Returns:
            str: delta in string form
        """
        if seconds is None:
            seconds = 0
        return str(datetime.timedelta(seconds=seconds))

    def get_enrichments(self):
        """
        Cycle through the OPERATIONAL_ENRICHMENTS and build the enrichments using callback to format/filter the results

        Returns:

        """
        try:
            self._snmp_connection = PanoptesSNMPConnectionFactory.get_snmp_connection(
                plugin_context=self._plugin_context, resource=self._device)
        except Exception as e:
            self._polling_status.handle_exception(u'operational', e)
            self._logger.warn(u'For Device {}, error connecting: {}'.format(self._device_name, repr(e)))

        operational_enrichment_set = PanoptesEnrichmentSet(u'operational')
        if self._snmp_connection is not None:
            for enrichment in self.OPERATIONAL_ENRICHMENTS:
                try:
                    enrichment_value = self.OPERATIONAL_ENRICHMENTS[enrichment]()

                    # Add the enrichment
                    if enrichment_value is not None:
                        self._polling_status.handle_success(enrichment)  # eg u'snmpenginetime'
                        operational_enrichment_set.add(enrichment, enrichment_value)
                        self._logger.debug(u'Operational enrichment for device {} {}: {} ({})'.format(
                            self._device_name, enrichment, enrichment_value, self.seconds_to_string(enrichment_value)))

                except Exception as e:
                    #  For everything that isn't a success
                    self._polling_status.handle_exception(enrichment, e)
                    self._logger.warn(u'For Device {}, error pulling {}: {}'.format(self._device_name, enrichment,
                                                                                    repr(e)))

        # Adding the device_polling_status - note that try/catch here could get recursive, so it's faith time.
        operational_enrichment_set.add(u'device_polling_status', self._polling_status.device_status)
        try:
            self._sysdescr_text = self._get_sysdescr()
            if self._sysdescr_text:
                self._sysdescr = sysdescrparser(self._sysdescr_text)
                operational_enrichment_set.add(u'sysdescr', self._sysdescr_text)
                operational_enrichment_set.add(u'device_vendor', self._sysdescr.vendor)
                operational_enrichment_set.add(u'device_model', self._sysdescr.model)
                operational_enrichment_set.add(u'device_os', self._sysdescr.os)
                operational_enrichment_set.add(u'device_os_version', self._sysdescr.version)
        except Exception as e:
            self._logger.warn(u'sysdescr parsing failed for resource: {}'.format(self._device_name))
            pass

        # Add the u'operational' set into the group, then add the group to the set.
        self._operational_enrichment_group.add_enrichment_set(operational_enrichment_set)
        self._operational_enrichment_group_set.add_enrichment_group(self._operational_enrichment_group)

        return self._operational_enrichment_group_set

    def run(self, context):
        """
        The main entry point to the plugin

        Args:
            context (PanoptesPluginContext): The Plugin Context passed by the Plugin Agent

        Returns:
            PanoptesEnrichmentGroupSet: A non-empty resource set
        """

        self._plugin_context = context
        self._conf = context.config
        self._logger = context.logger
        self._device = context.data
        self._device_name = self._device.resource_endpoint

        self._execute_frequency = int(self._conf[u'main'][u'execute_frequency'])
        self._enrichment_ttl = int(self._conf[u'main'][u'enrichment_ttl'])

        self._polling_status = PanoptesPollingStatus(resource=self._device, execute_frequency=self._execute_frequency,
                                                     logger=self._logger)
        self._operational_enrichment_group = \
            PanoptesOperationalEnrichmentGroup(enrichment_ttl=self._enrichment_ttl,
                                               execute_frequency=self._execute_frequency)
        self._operational_enrichment_group_set = PanoptesEnrichmentGroupSet(self._device)

        start_time = time.time()
        self._logger.info(u'Going to poll resource "{}" for operational enrichment'.format(self._device_name))
        device_results = self.get_enrichments()
        end_time = time.time()

        if device_results:
            self._logger.info(
                u'Done polling operational enrichment for resource "{}" in {:.2f} seconds, {} elements'.format(
                    self._device_name, end_time - start_time, len(device_results)))
        else:
            self._logger.warn(u'Error polling operational enrichment for resource {}'.format(self._device_name))

        return device_results
