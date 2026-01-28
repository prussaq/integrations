import logging
import time
import json
import hashlib
from curl_cffi import requests as cffi_requests
from urllib.parse import urlencode

from integrations.shared import rate_limiter
from integrations.shared.exceptions import ApiError
from integrations.shared.functions import execute_request

TIMEOUT = (5, 10)
FUTURES_BASE_URL = 'https://futures.mexc.com'

logger = logging.getLogger(__name__)

def sign_headers(headers, api, data):
    def gen_signature(timestamp, data):
        def md5(value): return hashlib.md5(value.encode('utf-8')).hexdigest()
        g = md5(api['uid'] + timestamp)[7:]
        s = json.dumps(data, separators=(',', ':'))
        return md5(timestamp + s + g)

    timestamp = str(int(time.time() * 1000))
    headers['x-mxc-sign'] = gen_signature(timestamp, data)
    headers['x-mxc-nonce'] = timestamp
    headers['User-Agent'] = 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36'
    headers['Authorization'] = api['uid']

def get_account_assets(api, **kwargs):
    """ 
    Get all account assets.

    Link: 
        https://www.mexc.com/api-docs/futures/account-and-trading-endpoints#get-all-account-assets
    Args:
        api (dict): API credentials
        kwargs (dict):
            timeout (float | (float, float)): HTTP timeout forwarded to `curl_cffi.requests` (connect/read).
            retries (int): Number of retry attempts.
            delay (float): Initial retry delay in seconds.
            backoff (float): Retry backoff multiplier.
            full (bool): If True, return both the parsed response body and the HTTP response object.
    Returns:
        dict: Parsed response body by default.
        (requests.Response, dict): When `full=True`, the HTTP response and the parsed body.
    Raises:
        RequestFailed: If the request fails due to a transport- or protocol-level failure.
        ApiError: If the response is semantically invalid or indicates an API-level error.
        Exception: Propagates any other unexpected exceptions.
    Notes: 
        Makes HTTP request by `curl_cffi` module.
    """
    url = f"{FUTURES_BASE_URL}/api/v1/private/account/assets"
    headers = {}
    timeout = kwargs.get('timeout', TIMEOUT)

    def send(): 
        sign_headers(headers, api)
        return cffi_requests.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        if not body.get('success'): 
            raise ApiError(f"MEXC returned code {body.get('code')}: {body.get('message')}", 
                response=response, body=body)

    try:
        rate_limiter.acquire('mexc.futures.web.get_account_assets') 
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to get account assets from MEXC: %s', e); raise

def place_order(api, data, **kwargs):
    """ 
    Place an order.
    Order volume is a number of contracts.

    Link: 
        https://www.mexc.com/api-docs/futures/account-and-trading-endpoints#place-order-under-maintenance
    Args:
        api (dict): WEB API credentials.
        data (dict): Request body parameters (JSON). See the documentation at `Link`.
        kwargs (dict):
            timeout (float | (float, float)): HTTP timeout forwarded to `curl_cffi.requests` (connect/read).
            full (bool): If True, return both the parsed response body and the HTTP response object.
    Returns:
        dict: Parsed response body by default.
        (requests.Response, dict): When `full=True`, the HTTP response and the parsed body.
    Raises:
        RequestFailed: If the request fails due to a transport- or protocol-level failure.
        ApiError: If the response is semantically invalid or indicates an API-level error.
        Exception: Propagates any other unexpected exceptions.
    Notes: 
        Makes HTTP request by `curl_cffi` module.
    """
    url = f"{FUTURES_BASE_URL}/api/v1/private/order/create"
    headers = {'Content-Type': 'application/json'}
    timeout = kwargs.get('timeout', TIMEOUT)

    try:
        rate_limiter.acquire('mexc.futures.web.place_order') 
        sign_headers(headers, api, data)
        response = cffi_requests.post(url, headers=headers, json=data, timeout=timeout)
        body = response.json()
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        if not body.get('success'): 
            raise ApiError(f"MEXC returned code {body.get('code')}: {body.get('message')}", 
                response=response, body=body)
        if kwargs.get('full'): return response, body
        return body
    except Exception as e: logger.error('Failed to place order on MEXC: %s', e); raise

def place_TP_SL_order(api, data, **kwargs):
    """ 
    Place take profit and stop loss order by position.

    Link: 
        https://www.mexc.com/api-docs/futures/account-and-trading-endpoints#place-tpsl-order-by-positionunder-maintenance
    Args:
        api (dict): WEB API credentials.
        data (dict): Request body parameters (JSON). See the documentation at `Link`.
        kwargs (dict):
            timeout (float | (float, float)): HTTP timeout forwarded to `curl_cffi.requests` (connect/read).
            full (bool): If True, return both the parsed response body and the HTTP response object.
    Returns:
        dict: Parsed response body by default.
        (requests.Response, dict): When `full=True`, the HTTP response and the parsed body.
    Raises:
        RequestFailed: If the request fails due to a transport- or protocol-level failure.
        ApiError: If the response is semantically invalid or indicates an API-level error.
        Exception: Propagates any other unexpected exceptions.
    Notes: 
        Makes HTTP request by `curl_cffi` module.
    """
    url = f"{FUTURES_BASE_URL}/api/v1/private/stoporder/place"
    headers = {'Content-Type': 'application/json'}
    timeout = kwargs.get('timeout', TIMEOUT)

    def send(): 
        sign_headers(headers, api, data)
        return cffi_requests.get(url, headers=headers, json=data, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        if not body.get('success'): 
            raise ApiError(f"MEXC returned code {body.get('code')}: {body.get('message')}", 
                response=response, body=body)

    try:
        rate_limiter.acquire('mexc.futures.web.place_TP_SL_order') 
        return execute_request(send, read, check, retries=1)
    except Exception as e: logger.error('Failed to get take profit and stop loss order on MEXC: %s', e); raise
