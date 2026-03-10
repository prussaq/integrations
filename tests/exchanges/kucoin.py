import logging
import os, sys
import json

APP_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
sys.path.insert(0, APP_DIR)
os.chdir(APP_DIR)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s [%(name)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

import integrations.tests.shared.config as config
from integrations.tests.shared.tools import load_secrets_file, get_api
from integrations.exchanges.kucoin.classic_rest.futures import market, funding_fees, positions
from integrations.exchanges.kucoin.classic_websocket.base_info.futures import get_private_token, get_public_token
from integrations.exchanges.kucoin.classic_rest.account import account_funding

secrets = load_secrets_file(config.SECRETS_PATH)
api = get_api(secrets, config.KUCOIN_API_PATH)

# data = market.get_symbol('OPUSDTM')
# data = funding_fees.get_private_funding_history(api, 'STOUSDTM')
# data = get_private_token(api)
# data = get_public_token()
# data = account_funding.get_futures_account(api)
# data = positions.get_position_list(api, {'currency': 'USDT'})
# data = positions.get_position_details(api, 'CUDISUSDTM')
data = positions.get_positions_history(api, {'symbol':'GOATUSDTM'})

print('data:', data)
