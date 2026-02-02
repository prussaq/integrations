import logging
import os, sys
import json

APP_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
sys.path.insert(0, APP_DIR)
os.chdir(APP_DIR)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s [%(name)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

import integrations.tests.shared.config as config
from integrations.tests.shared.tools import load_secrets_file, get_api
from integrations.exchanges.bybit.v5 import market, position, account, trade

secrets = load_secrets_file(config.SECRETS_PATH)
api = get_api(secrets, config.BYBIT_API_PATH)

# data = market.get_kline('OPUSDT', 'D', {'limit': 10})
# data = market.get_instruments_info('linear', {'symbol': 'ORBSUSDT'})
# data = market.get_tickers('linear', {'symbol': 'ORBSUSDT'})
# data = market.get_funding_rate_history('linear', symbol='OPUSDT')
# data = position.get_position_info(api, 'linear', {'symbol': 'YZYUSDT'})
# data = position.set_leverage(api, 'linear', 'OPUSDT', buy='1', sell='1')
# data = position.get_closed_PnL(api, 'linear', {'symbol': 'YZYUSDT', 'limit':100})
# data = account.get_transferable_amount_unified(api, 'USDT')
# data = account.get_transaction_log(api)
data = account.get_account_info(api)

print('data:', data)
