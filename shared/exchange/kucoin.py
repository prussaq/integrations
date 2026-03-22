import time
import hmac
import hashlib
import base64

TIMEOUT = (5, 10)

FUTURES_BASE_URL = 'https://api-futures.kucoin.com'

def sign_headers(headers, api, method, endpoint, body=''):
    """
    Signs headers to call a private endpoints.

    Args:
        headers (dict): Headers to sign.
        api (dict): 
            key (str): API key.
            secret (str): API secret.
            version (str): API version.
            passphrase (str): passphrase.
        method (str): HTTP method (e.g., "GET", "POST", "DELETE").
        endpoint (str): Endpoint including request parameters.
        body (str): String representation of JSON payload.
    """
    timestamp = str(int(time.time() * 1000))
    str_to_sign = timestamp + method + endpoint + body
    signature = base64.b64encode(hmac.new(api['secret'].encode('utf-8'), str_to_sign.encode('utf-8'), hashlib.sha256).digest()).decode('utf-8')
    passphrase = base64.b64encode(hmac.new(api['secret'].encode('utf-8'), api['passphrase'].encode('utf-8'), hashlib.sha256).digest()).decode('utf-8')
    headers["KC-API-KEY"] = api['key']
    headers["KC-API-SIGN"]= signature
    headers["KC-API-TIMESTAMP"] = timestamp
    headers["KC-API-PASSPHRASE"] = passphrase
    headers["KC-API-KEY-VERSION"] = api['version']