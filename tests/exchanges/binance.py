import logging
import os, sys
import json

APP_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
sys.path.insert(0, APP_DIR)
os.chdir(APP_DIR)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s [%(name)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

import integrations.tests.shared.config as config
from integrations.tests.shared.tools import load_secrets_file, get_api
import integrations.exchanges.binance.derivatives.usdsm_futures.market_data.rest as market_data

secrets = load_secrets_file(config.SECRETS_PATH)
# api = get_api(secrets, config.BINANCE_API_PATH)

# data = market_data.get_kline('OPUSDT', '1d', {'limit': 10})
# data = market_data.get_funding_rate_history({'symbol': 'OPUSDT'})
# data = market_data.get_funding_rate_info()
data = market_data.get_price_ticker_v2()

print('data:', data)
