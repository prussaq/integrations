import hmac
import base64
import time

TIMEOUT = (5, 10)

MAIN_DOMAIN = 'https://api.bitget.com'

def sign_headers(headers, api, method, path, body='', *, locale='en-US'):
    """
    Signs headers to call a private endpoints.

    Args:
        headers (dict): Headers to sign.
        api (dict): 
            access_key (str): API key.
            secret_key (str): API secret.
            passphrase (str): passphrase.
        method (str): HTTP method (e.g., "GET", "POST", "DELETE").
        path (str): Path including request parameters; must be sorted in ascending alphabetical order by key.
        body (str): String representation of JSON payload.
        locale (str): Support language such as: Chinese (zh-CN), English (en-US)
    """
    timestamp = str(int(time.time() * 1000))
    str_to_sign = timestamp + method + path + body
    signature = base64.b64encode(hmac.new(bytes(api['secret_key'], encoding='utf8'), bytes(str_to_sign, encoding='utf-8'), digestmod='sha256').digest())
    headers["ACCESS-KEY"] = api['access_key']
    headers["ACCESS-SIGN"]= signature
    headers["ACCESS-TIMESTAMP"] = timestamp
    headers["ACCESS-PASSPHRASE"] = api['passphrase']
    headers["locale"] = locale
    if method == 'POST': headers['Content-Type'] = 'application/json'
