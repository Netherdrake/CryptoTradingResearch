import requests as rs
from requests.exceptions import RequestException
from funcy.flow import retry, silent

@retry(3, errors=RequestException, timeout=lambda a: 3 ** a)  # 27 sec max delay
def get_safe(url, session=None):
    """ Get a single url, /w auto-retry. """
    handler = session if session else rs
    resp = handler.get(url, timeout=30)

    if resp and hasattr(resp, "status_code") and resp.status_code == 200:
        return resp


def get_multi(urls):
    """ A generator of successful url fetching responses. """
    with rs.Session() as s:
        for url in urls:
            resp = silent(get_safe)(url, session=s)
            if resp:
                yield resp

