import time
import hmac
import hashlib

TIMEOUT = (5, 10)
RECV_WINDOW = '5000'

BASE_URL = 'https://api.bybit.com'

def sign_headers(headers, api, recv_window, body=''):
    """
    Signs headers to call a private endpoints.

    Args:
        headers (dict): Headers to sign.
        api (dict): 
            key (str): API key.
            secret (str): API secret.
        recv_window (str): Exchange receive window (ms).
        body (str): String representation of JSON payload.
    """
    timestamp = str(int(time.time() * 1000))
    str_to_sign= str(timestamp) + api['key'] + recv_window + body
    hash_data = hmac.new(bytes(api['secret'], "utf-8"), str_to_sign.encode("utf-8"), hashlib.sha256)
    signature = hash_data.hexdigest()
    headers['X-BAPI-API-KEY'] = api['key']
    headers['X-BAPI-SIGN'] = signature
    headers['X-BAPI-SIGN-TYPE'] = '2'
    headers['X-BAPI-TIMESTAMP'] = timestamp
    headers['X-BAPI-RECV-WINDOW'] = recv_window

