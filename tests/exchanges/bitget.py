import logging
import os, sys
import json

APP_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
sys.path.insert(0, APP_DIR)
os.chdir(APP_DIR)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s [%(name)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

import integrations.tests.shared.config as config
from integrations.tests.shared.tools import load_secrets_file, get_api
from integrations.exchanges.bitget.futures import market, account, position

secrets = load_secrets_file(config.SECRETS_PATH)
api = get_api(secrets, config.BITGET_API_PATH)

# data = market.get_ticker('OPUSDT', 'USDT-FUTURES')
# data = market.get_candlestick_data('OPUSDT', 'USDT-FUTURES', '1Dutc')
# data = market.get_next_funding_time('OPUSDT', 'USDT-FUTURES')
# data = market.get_historical_funding_rates('OPUSDT', 'USDT-FUTURES', {'pageSize':2})
# data = market.get_current_funding_rate('USDT-FUTURES', {'symbol':'OPUSDT'})
# data = account.get_single_account(api, 'OPUSDT','USDT-FUTURES', 'USDT')
data = position.get_single_position(api, 'OPUSDT','USDT-FUTURES', 'USDT')

print('data:', data)
