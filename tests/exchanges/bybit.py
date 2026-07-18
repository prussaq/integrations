import logging
import os, sys
import json

APP_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
sys.path.insert(0, APP_DIR)
os.chdir(APP_DIR)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s [%(name)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

import integrations.tests.shared.config as config
from integrations.tests.shared.tools import get_api
from integrations.exchanges.bybit.v5 import market, position, account, trade

api = get_api(config.CREDS, config.BYBIT_API)

# data = market.get_kline('OPUSDT', 'D', {'limit': 10})
# data = market.get_instruments_info('linear', {'symbol': 'ORBSUSDT'})
# data = market.get_tickers('linear', {'symbol': 'ORBSUSDT'})
# data = market.get_funding_rate_history('linear', symbol='OPUSDT')
# data = position.get_position_info(api, 'linear', {'symbol': 'YZYUSDT'})
# data = position.set_leverage(api, 'linear', 'OPUSDT', buy='1', sell='1')
# data = position.get_closed_PnL(api, 'linear', {'symbol': 'YZYUSDT', 'limit':100})
# data = account.get_transferable_amount_unified(api, 'USDT')
# data = account.get_transaction_log(api, {'symbol': '1000BONKPERP'})
# data = account.get_account_info(api)

# base = leg['instrument']['symbol'].replace('USDT', '')
params = {
    "accountType": 'UNIFIED', 
    "category": 'linear', "type": 'SETTLEMENT', 
    "baseCoin": '1000BONK', "currency": '' }
body = account.get_transaction_log(api, params)
print('body:', body)

# print('data:', data)
