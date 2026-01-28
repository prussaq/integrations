import logging
import os, sys
import json

APP_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
sys.path.insert(0, APP_DIR)
os.chdir(APP_DIR)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s [%(name)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

import integrations.tests.shared.config as config
from integrations.tests.shared.tools import load_secrets_file, get_api
import integrations.exchanges.okx.api.public_data.rest as public_data
import integrations.exchanges.okx.api.trading_account.rest as trading_account

secrets = load_secrets_file(config.SECRETS_PATH)
api = get_api(secrets, config.OKX_API_PATH)

# data = public_data.get_instruments('SWAP', {'instId': 'OP-USDT-SWAP'})
# data = public_data.get_funding_rate('OP-USDT-SWAP')
# data = public_data.get_funding_rate_history('OP-USDT-SWAP', {'limit': 10})
# data = public_data.get_mark_price('SWAP', {'instId': 'OP-USDT-SWAP'})
# data = trading_account.get_positions(api, {'instType': 'SWAP', 'instId': 'OP-USDT-SWAP'})
# data = trading_account.get_balance(api, {'ccy': 'USDT'})
# data = trading_account.get_balance(api)
data = trading_account.set_leverage(api, '4', 'isolated', {'instId': 'OP-USDT-SWAP'})

print('data:', data)
