import six
from scrapy.utils.misc import load_object
import elasticsearch


# Shortcut maps 'setting name' -> 'parmater name'.
SETTINGS_PARAMS_MAP = {
    'ELS_HOST': 'ELS_HOST',
}
ELS_PARAMS = {
    'PORT': 'PORT',
}

def get_els_from_settings(settings):

    params = ELS_PARAMS.copy()
    params.update(settings.getdict('ELS_PARAMS'))

    for source, dest in SETTINGS_PARAMS_MAP.items():
        val = settings.get(source)
        if val:
            params[dest] = val

    if isinstance(params.get('els_cls'), six.string_types):
        params['els_cls'] = load_object(params['els_cls'])

    return get_els(**params)


# Backwards compatible alias.
from_settings = get_els_from_settings


def get_els(**kwargs):
    """Returns a Elasticsearch client instance.
    Returns
    -------
    server
        Elasticsearch client instance.
    """
    els_host = kwargs.get('ELS_HOST')

    els_cls = kwargs.pop('els_cls', elasticsearch.Elasticsearch)
    # Insert Your Elasticsearch server IP or Domain
    return els_cls([''],timeout=30, max_retries=10, retry_on_timeout=True)

