import logging
import os, sys
import json

APP_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(__file__)))))
sys.path.insert(0, APP_DIR)
os.chdir(APP_DIR)
logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s [%(name)s] %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

import integrations.tests.shared.config as config
from integrations.tests.shared.tools import load_secrets_file, get_api
from integrations.exchanges.htx.new.usdtm_futures import reference_data, market_data, account, unified_account

secrets = load_secrets_file(config.SECRETS_PATH)
api = get_api(secrets, config.HTX_API_PATH)

# data = reference_data.query_funding_rate('OP-USDT')
# data = reference_data.query_batch_funding_rate({'contract_code': 'OP-USDT'})
# data = reference_data.query_historical_funding_rate('OP-USDT')
# data = reference_data.query_contract_info({'contract_code': 'OP-USDT'})
# data = reference_data.query_contract_elements({'contract_code': 'OP-USDT'})
# data = market_data.get_kline_data('OP-USDT', '1day', {'size': 1})
# data = market_data.get_market_depth('OP-USDT', 'step0')
# data = market_data.get_market_BBO_data({'contract_code': 'OP-USDT'})
data = market_data.get_last_trade({'contract_code': 'OP-USDT'})
# data = account.query_asset_valuation(api, 'USDT')
# data = account.query_account_info_isolated(api, {'contract_code': 'OP-USDT'})
# data = unified_account.query_unified_account_assets(api)

# try: data = unified_account.query_unified_account_assets(api)
# except Exception as e: print(e.body)

print('data:', data)
