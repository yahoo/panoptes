"""
Copyright 2018, Oath Inc.
Licensed under the terms of the Apache 2.0 license. See LICENSE file in project root for terms.
"""
from yahoo_panoptes.framework import const
from yahoo_panoptes.framework.utilities.key_value_store import PanoptesKeyValueStore
from yahoo_panoptes.framework.validators import PanoptesValidators


class PanoptesSecretsStore(PanoptesKeyValueStore):
    """
    A custom Key/Value store for secrets
    """

    def __init__(self, context):
        super(PanoptesSecretsStore, self).__init__(context, const.SECRETS_MANAGER_KEY_VALUE_NAMESPACE)

    def get_by_site(self, secret_name, site, fallback_to_default=True):
        assert PanoptesValidators.valid_nonempty_string(secret_name), u'secret_name must be a non-empty str or unicode'
        assert PanoptesValidators.valid_nonempty_string(site), u'site must be a non-empty str or unicode'
        assert type(fallback_to_default) == bool, u'fallback_to_default must be a boolean'

        secret_key = secret_name + u':' + site
        try:
            secret = super(PanoptesSecretsStore, self).get(key=secret_key)
            return secret
        except Exception as e:
            if not fallback_to_default:
                raise e

        # If we didn't find a site based key AND fallback_to_default is set to true
        secret_key = secret_name + u':default'
        try:
            secret = super(PanoptesSecretsStore, self).get(key=secret_key)
            return secret
        except Exception as e:
            raise e

    # These methods should be inoperable for the secrets store.
    def set(self, key, value, expire=None):
        pass

    def delete(self, key):
        pass

    def set_add(self, set_name, member):
        pass

    def set_members(self, set_name):
        pass
