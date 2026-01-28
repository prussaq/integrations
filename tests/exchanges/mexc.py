import logging
import os, sys
import json

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s [%(name)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
logger = logging.getLogger(__name__)
APP_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
sys.path.insert(0, APP_DIR)
os.chdir(APP_DIR)

import integrations.tests.shared.config as config
from integrations.tests.shared.tools import load_secrets_file, get_api
from integrations.exchanges.mexc.futures import market, account_trading

secrets = load_secrets_file(config.SECRETS_PATH)
api = get_api(secrets, config.MEXC_API_PATH)

# data = market.get_contract_info({'symbol': 'PRCL_USDT'})
# data = market.get_contract_info()
# data = account_trading.get_currency_asset(api, 'USDT')
# data = account_trading.get_account_assets(api)
data = account_trading.get_open_positions(api)

print('data:', data)
