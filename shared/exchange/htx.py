import base64
import hashlib
import hmac
import time
import urllib.parse

TIMEOUT = (5, 10)

FUTURES_BASE_URLS = {
    'standard': 'https://api.hbdm.com',
    'aws': 'https://api.hbdm.vn'
}
FUTURES_BASE_URL = FUTURES_BASE_URLS['standard']

def sign_params(params, api, method, host, path):
    """
    Signs params to call a private endpoints.

    Args:
        params (dict): Params to sign.
        api (dict): 
            access_key (str): API key.
            secret_key (str): API secret.
        method (str): HTTP method (e.g., "GET", "POST", "DELETE").
        host (str): Host to call, e.g. "api.hbdm.com".
        path (str): Resource path.
    """
    params["AccessKeyId"] = api['access_key']
    params["SignatureMethod"] = "HmacSHA256"
    params["SignatureVersion"] = "2"
    params["Timestamp"] = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
    sorted_params = sorted(params.items(), key=lambda d: d[0])
    encoded_params = urllib.parse.urlencode(sorted_params)
    payload = f"{method}\n{host}\n{path}\n{encoded_params}"
    digest = hmac.new(api['secret_key'].encode('utf-8'), payload.encode('utf-8'), hashlib.sha256).digest()
    params['Signature'] = base64.b64encode(digest).decode()

