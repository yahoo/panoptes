from yahoo_panoptes.polling.polling_plugin import PanoptesPollingPlugin


class PanoptesTestPollingPlugin(PanoptesPollingPlugin):
    def run(self, context):
        super(PanoptesTestPollingPlugin, self).run(context)
