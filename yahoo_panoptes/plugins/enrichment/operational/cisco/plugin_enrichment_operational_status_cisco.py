from yahoo_panoptes.plugins.enrichment.operational.plugin_enrichment_operational_status import \
    OperationalEnrichment


class CiscoOperationalEnrichment(OperationalEnrichment):
    """Cisco-based enrichments"""

    def _callback_last_updated(self):
        """
        This is a built metric from a complex interplay of ccmHistoryRunningLastChanged, ccmHistoryRunningLastSaved
        and sysUpTime that *only* has context for Cisco devices

        The first two are the _value of sysUpTime_ when the system was changed and saved.  The distinction between the
        two is that it's possible to be changed, but not saved.

        For the purposes of this enrichment, we only care about the answer to the question, when was this last updated?

        Returns:
            int: seconds since last _save_
        """
        save_delta = self._get_cisco_last_saved()
        base = self._get_sysuptime()
        seconds = base - save_delta

        return seconds

    def _get_cisco_last_changed(self):
        """
        Cisco Enrichment only
        https://iphostmonitor.com/mib/oids/CISCO-CONFIG-MAN-MIB/ccmHistoryRunningLastChanged.html

        The value of sysUpTime when the running configuration was last changed. If the value of
        ccmHistoryRunningLastChanged is greater than ccmHistoryRunningLastSaved, the configuration has been changed
        but not saved.

        Not currently used because I'm bothered about the 'save', but it could be useful for flagging out of date and
        unsaved configurations more widely.  Code quality buffer for the moment.

        Returns:
            int: seconds since the last configuration change
        """

        # oid = '.1.3.6.1.4.1.9.9.43.1.1.1.0'
        # try:
        #    timeticks = int(self._get_oid_result(oid))
        # except Exception as e:
        #    raise e

        # return self.timeticks_to_seconds(timeticks)
        pass

    def _get_cisco_last_saved(self):
        """
        Cisco Enrichment only
        https://iphostmonitor.com/mib/oids/CISCO-CONFIG-MAN-MIB/ccmHistoryRunningLastSaved.html

        The value of sysUpTime when the running configuration was last saved (written). If the value of
        ccmHistoryRunningLastChanged is greater than ccmHistoryRunningLastSaved, the configuration has been changed but
        not saved. What constitutes a safe saving of the running configuration is a management policy issue beyond the
        scope of this MIB. For some installations, writing the running configuration to a terminal may be a way of
        capturing and saving it. Others may use local or remote storage. Thus ANY write is considered saving for the
        purposes of the MIB.

        Returns:
            int: seconds since the last configuration save
        """

        oid = u'.1.3.6.1.4.1.9.9.43.1.1.2.0'
        try:
            timeticks = int(self._get_oid_result(oid))
        except Exception as e:
            raise e

        return self.timeticks_to_seconds(timeticks)
