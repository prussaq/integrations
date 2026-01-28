import datetime
import hmac
import hashlib
import base64

TIMEOUT = (5, 10)

BASE_URL = "https://www.okx.com"

def sign_headers(headers, api, method, endpoint, body='', ttl=None):
    """
    Signs headers to call a private endpoints.

    Args:
        headers (dict): Headers to sign.
        api (dict): 
            key (str): API key.
            secret (str): API secret.
            passphrase (str): API passphrase.
        method (str): HTTP method (e.g., "GET", "POST", "DELETE").
        endpoint (str): Endpoint including request parameters.
        body (str): String representation of JSON payload.
        ttl (int): Request receive window in milliseconds.
    """
    now = datetime.datetime.now(datetime.timezone.utc)
    timestamp = now.isoformat(timespec="milliseconds")[:-6] + 'Z'
    str_to_sign = f"{timestamp}{method}{endpoint}{body}"
    signature = base64.b64encode(hmac.new(api['secret'].encode('utf-8'), str_to_sign.encode('utf-8'), hashlib.sha256).digest()).decode('utf-8')
    headers["OK-ACCESS-KEY"] = api['key']
    headers["OK-ACCESS-SIGN"] = signature
    headers["OK-ACCESS-TIMESTAMP"] = timestamp
    headers["OK-ACCESS-PASSPHRASE"] = api['passphrase']
    if ttl:  headers["expTime"] = str(int(now.timestamp() * 1000) + ttl)
