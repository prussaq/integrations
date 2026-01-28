import logging
import json
import requests
from urllib.parse import urlencode

from integrations.shared import rate_limiter
from integrations.shared.exceptions import ApiError
from integrations.shared.functions import execute_request
import integrations.shared.exchange.kucoin as kucoin

logger = logging.getLogger(__name__)

def add_order(api, data, *, headers={}, **kwargs):
    """ 
    Place futures order.

    Link: 
        https://www.kucoin.com/docs-new/rest/futures-trading/orders/add-order
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
        data (dict): Request body parameters (JSON). See the documentation at `Link`.
        headers (dict): HTTP headers.
        kwargs (dict):
            session (requests.Session): Must be managed by caller.
            base_url (str): Base HTTP endpoint for the exchange API.
            timeout (float | (float, float)): HTTP timeout forwarded to `requests` (connect/read).
            full (bool): If True, return both the parsed response body and the HTTP response object.
    Returns:
        dict: Parsed response body by default.
        (requests.Response, dict): When `full=True`, the HTTP response and the parsed body.
    Raises:
        RequestFailed: If the request fails due to a transport- or protocol-level failure.
        ApiError: If the response is semantically invalid or indicates an API-level error.
        Exception: Propagates any other unexpected exceptions.
    Notes: 
        Makes HTTP request by `requests` or `requests.Session` if provided.
    """
    http = kwargs.get('session', requests)
    base_url = kwargs.get('base_url', kucoin.FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', kucoin.TIMEOUT)
    method = 'POST'
    endpoint = f"/api/v1/orders"
    url = base_url + endpoint
    payload = json.dumps(data, separators=(',', ':'))
    headers['Content-Type'] = 'application/json'

    try:
        rate_limiter.acquire('kucoin.classic_rest.futures.orders.add_order')
        kucoin.sign_headers(headers, api, method, endpoint, payload)
        response = http.post(url, data=payload, headers=headers, timeout=timeout)
        body = response.json()
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '200000': 
            raise ApiError(f"KuCoin returned code {code}: {body.get('msg')}", response=response, body=body)
        if kwargs.get('full'): return response, body
        return body
    except Exception as e: logger.error('Failed to add order on KuCoin: %s', e); raise

def add_TP_SL_order(api, data, *, headers={}, **kwargs):
    """ 
    Place take profit and stop loss order.

    Link: 
        https://www.kucoin.com/docs-new/rest/futures-trading/orders/add-take-profit-and-stop-loss-order
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
        data (dict): Request body parameters (JSON). See the documentation at `Link`.
        headers (dict): HTTP headers.
        kwargs (dict):
            session (requests.Session): Must be managed by caller.
            base_url (str): Base HTTP endpoint for the exchange API.
            timeout (float | (float, float)): HTTP timeout forwarded to `requests` (connect/read).
            full (bool): If True, return both the parsed response body and the HTTP response object.
    Returns:
        dict: Parsed response body by default.
        (requests.Response, dict): When `full=True`, the HTTP response and the parsed body.
    Raises:
        RequestFailed: If the request fails due to a transport- or protocol-level failure.
        ApiError: If the response is semantically invalid or indicates an API-level error.
        Exception: Propagates any other unexpected exceptions.
    Notes: 
        Makes HTTP request by `requests` or `requests.Session` if provided.
    """
    http = kwargs.get('session', requests)
    base_url = kwargs.get('base_url', kucoin.FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', kucoin.TIMEOUT)
    method = 'POST'
    endpoint = f"/api/v1/st-orders"
    url = base_url + endpoint
    payload = json.dumps(data, separators=(',', ':'))
    headers['Content-Type'] = 'application/json'

    def send(): 
        kucoin.sign_headers(headers, api, method, endpoint, payload)
        return http.post(url, data=payload, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '200000': 
            raise ApiError(f"KuCoin returned code {code}: {body.get('msg')}", response=response, body=body)

    try: 
        rate_limiter.acquire('kucoin.classic_rest.futures.orders.add_TP_SL_order') 
        return execute_request(send, read, check, retries=1)
    except Exception as e: logger.error('Failed to add take profit and stop loss order on KuCoin: %s', e); raise
