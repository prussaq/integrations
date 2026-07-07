import logging
import os, sys
import json

APP_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
sys.path.insert(0, APP_DIR)
os.chdir(APP_DIR)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s [%(name)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

import integrations.tests.shared.config as config
from integrations.tests.shared.tools import get_api
from integrations.exchanges.dydx.indexer.http import markets 

# api = get_api(config.CREDS, config.DYDX_API)

# data = markets.get_perpetual_markets({'limit':1})
# data = markets.get_candles('BTC-USD', '1DAY', {'limit':1})
data = markets.get_candles('BTC-USD', '1HOUR', {'limit':5})

print('data:', data)
