import time
import hmac
import hashlib

TIMEOUT = (5, 10)

FUTURES_BASE_URL = 'https://contract.mexc.com'

def sign_headers(headers, api, method, *, query='', body=''):
    """
    Signs headers to call a private endpoints.

    Args:
        headers (dict): Headers to sign.
        api (dict):
            key (str): API key.
            secret (str): API secret.
        method (str): HTTP method (e.g., "GET", "POST", "DELETE").
        query (str): Request query string (parameters). Must be sorted in ascending alphabetical order by key.
        body (str): String representation of JSON payload.
    """
    timestamp = str(int(time.time() * 1000))
    if method.upper() in ["GET", "DELETE"]: str_to_sign = api['key'] + timestamp + query
    elif method.upper() == "POST": str_to_sign = api['key'] + timestamp + body
    else: raise ValueError(f"unsupported HTTP method: {method}")
    signature = hmac.new(api['secret'].encode('utf-8'), str_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()
    headers["Request-Time"] = timestamp
    headers["ApiKey"] = api['key']
    headers["Signature"] = signature
