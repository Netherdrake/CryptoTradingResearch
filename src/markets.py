import os
import requests
import datetime as dt
from bs4 import BeautifulSoup
from collections import defaultdict
from funcy import walk_values, join, flatten, first, rest
from contextlib import suppress
import pickle
import pandas as pd
import numpy as np

import plotly.plotly as py
import plotly.graph_objs as go
import cufflinks as cf
from toolz import keyfilter

# helpers
# -------
def keep(d, whitelist):
    return keyfilter(lambda k: k in whitelist, d)

def omit(d, blacklist):
    return keyfilter(lambda k: k not in blacklist, d)

# coinmarket cap
# --------------
def coincap():
    return requests.get('http://www.coincap.io/front').json()

def coincap_coin(symbol):
    return requests.get('http://www.coincap.io/page/%s' % symbol.upper()).json()

def all_coins():
    r = requests.get('http://coinmarketcap.northpole.ro/api/v6/all.json')
    return r.json()

def get_coin(symbol):
    r = requests.get('http://coinmarketcap.northpole.ro/api/v6/%s.json' % symbol.upper())
    return r.json()

def top_coins(limit=100):
    all_coin = all_coins()
    coins = [x for x in all_coin['markets'] if int(x['position']) < limit]
    return coins[::-1]

def historic_urls():
    history_root = 'http://coinmarketcap.northpole.ro/api/v6/history/'
    body = requests.get(history_root).text
    soup = BeautifulSoup(body, 'lxml')
    links = soup.find_all('a')[5:]
    return ['{}{}'.format(history_root, x['href']) for x in links]

def get_historic_data(urls=None):
    if not urls:
        urls = historic_urls()

    rs = (requests.get(u, timeout=60) for u in urls)
    return [x.json() for x in rs
            if x and hasattr(x, "status_code") and x.status_code == 200 and x.json()]

def merge_historic_data(historic_data):
    """ Simplify and flatten all historic data into a single list of events."""
    data = []
    for hist in historic_data:
        data.append([simplify_fragment(x) for x in hist['history'].values()])
    return list(filter(bool, flatten(data)))

def store_historic_data(historic_data):
    if not os.path.exists('data'):
        os.makedirs('data')

    with open('data/dump.pickle', 'wb') as p:
        pickle.dump(historic_data, p)

def read_historic_data():
    with open('data/dump.pickle', 'rb') as p:
        return pickle.load(p)

def simplify_fragment(obj):
    """ Simplify and flatten individual fragment."""
    # clean up the mess
    def replace_values(val):
        if type(val) == dict:
            return walk_values(replace_values, val)
        if val == "?" or val == 'None':
            return 0
        return val
    obj = walk_values(replace_values, obj)

    result = None
    with suppress(Exception):
        result = {
            'symbol': obj['symbol'],
            'category': obj['category'],
            'supply': obj['availableSupply'],
            'change_7d': round(float(obj['change7d']), 2),
            'change_1d': round(float(obj['change24h']), 2),
            'change_1h': round(float(obj['change1h']), 2),
            'position': int(obj['position']),
            'cap_usd': round(float(obj['marketCap']['usd'])),
            'cap_btc': round(float(obj['marketCap']['btc'])),
            'volume_btc': round(float(obj['volume24']['btc'])),
            'price_usd': float(obj['price']['usd']),
            'price_btc': float(obj['price']['btc']),
            'timestamp': dt.datetime.fromtimestamp(obj['timestamp'])
        }
    return result

def refresh_data(urls=None):
    data = get_historic_data(urls=urls)
    data = merge_historic_data(data)
    store_historic_data(data)

def simplify_hist_data(historic_data):
    return [keep(x, ['symbol', 'timestamp', 'price_usd']) for x in historic_data]


if __name__ == '__main__':
    pass
