from six import string_types

from .. import const
from .key_value_store import PanoptesKeyValueStore


class PanoptesSecretsStore(PanoptesKeyValueStore):
    """
    A custom Key/Value store for secrets
    """

    def __init__(self, context):
        super(PanoptesSecretsStore, self).__init__(context, const.SECRETS_MANAGER_KEY_VALUE_NAMESPACE)

    def get_by_site(self, secret_name, site, fallback_to_default=True):
        assert secret_name and isinstance(secret_name, string_types), 'secret_name must be a non-empty str or unicode'
        assert site and isinstance(site, string_types), 'site must be a non-empty str or unicode'
        assert type(fallback_to_default) == bool, 'fallback_to_default must be a boolean'

        secret_key = secret_name + ':' + site
        try:
            secret = super(PanoptesSecretsStore, self).get(key=secret_key)
            return secret
        except Exception as e:
            if not fallback_to_default:
                raise e

        # If we didn't find a site based key AND fallback_to_default is set to true
        secret_key = secret_name + ':default'
        try:
            secret = super(PanoptesSecretsStore, self).get(key=secret_key)
            return secret
        except Exception as e:
            raise e

    def set(self, key, value, expire=None):
        pass

    def delete(self, key):
        pass

    def set_add(self, set_name, member):
        pass

    def set_members(self, set_name):
        pass
