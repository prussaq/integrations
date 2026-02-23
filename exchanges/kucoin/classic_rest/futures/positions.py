import logging
import json
import requests
from urllib.parse import urlencode

from integrations.shared import rate_limiter
from integrations.shared.exceptions import ApiError
from integrations.shared.functions import execute_request
import integrations.shared.exchange.kucoin as kucoin

logger = logging.getLogger(__name__)


def get_position_details(api, symbol, *, headers={}, **kwargs):
    """ 
    Get position details by symbol.

    Link: 
        https://www.kucoin.com/docs-new/rest/futures-trading/positions/get-position-details
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
        symbol (str): Symbol of the contract.
        headers (dict): HTTP headers.
        kwargs:
            session (requests.Session): Must be managed by caller.
            base_url (str): Base HTTP endpoint for the exchange API.
            timeout (float | (float, float)): HTTP timeout forwarded to `requests` (connect/read).
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
        Makes HTTP request by `requests` or `requests.Session` if provided.
    """
    http = kwargs.get('session', requests)
    base_url = kwargs.get('base_url', kucoin.FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', kucoin.TIMEOUT)
    method = 'GET'
    endpoint = f"/api/v2/position?symbol={symbol}"
    url = base_url + endpoint

    def send(): 
        kucoin.sign_headers(headers, api, method, endpoint)
        return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '200000': 
            raise ApiError(f"KuCoin returned code {code}: {body.get('msg')}", response=response, body=body)

    rate_limiter.acquire('kucoin.classic_rest.futures.positions.get_position_details')
    return execute_request(send, read, check, kwargs)


def get_position_list(api, params={}, *, headers={}, **kwargs):
    """ 
    Get position list by currency.

    Link: 
        https://www.kucoin.com/docs-new/rest/futures-trading/positions/get-position-list
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
        params (dict):
            currency (str): Currency name, e.g. USDT, XBT. Default: All
        headers (dict): HTTP headers.
        kwargs:
            session (requests.Session): Must be managed by caller.
            base_url (str): Base HTTP endpoint for the exchange API.
            timeout (float | (float, float)): HTTP timeout forwarded to `requests` (connect/read).
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
        Makes HTTP request by `requests` or `requests.Session` if provided.
    """
    http = kwargs.get('session', requests)
    base_url = kwargs.get('base_url', kucoin.FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', kucoin.TIMEOUT)
    method = 'GET'
    endpoint = '/api/v1/positions' + (f"?{urlencode(params)}" if params else '')
    url = base_url + endpoint

    def send(): 
        kucoin.sign_headers(headers, api, method, endpoint)
        return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '200000': 
            raise ApiError(f"KuCoin returned code {code}: {body.get('msg')}", response=response, body=body)

    rate_limiter.acquire('kucoin.classic_rest.futures.positions.get_position_list')
    return execute_request(send, read, check, kwargs)


def get_positions_history(api, params={}, *, headers={}, **kwargs):
    """ 
    Get positions history.

    Link: 
        https://www.kucoin.com/docs-new/rest/futures-trading/positions/get-positions-history
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
        params (dict):
            symbol (str): Symbol of the contract, Please refer to Get Symbol endpoint: symbol
            from (int): Closing start time(ms)
            to (int): Closing end time(ms)
            limit (int): Number of requests per page, max 200, default 10
            pageId (int): Current page number, default 1
        headers (dict): HTTP headers.
        kwargs:
            session (requests.Session): Must be managed by caller.
            base_url (str): Base HTTP endpoint for the exchange API.
            timeout (float | (float, float)): HTTP timeout forwarded to `requests` (connect/read).
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
        Makes HTTP request by `requests` or `requests.Session` if provided.
    """
    http = kwargs.get('session', requests)
    base_url = kwargs.get('base_url', kucoin.FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', kucoin.TIMEOUT)
    method = 'GET'
    endpoint = '/api/v1/history-positions' + (f"?{urlencode(params)}" if params else '')
    url = base_url + endpoint

    def send(): 
        kucoin.sign_headers(headers, api, method, endpoint)
        return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '200000': 
            raise ApiError(f"KuCoin returned code {code}: {body.get('msg')}", response=response, body=body)

    rate_limiter.acquire('kucoin.classic_rest.futures.positions.get_position_list')
    return execute_request(send, read, check, kwargs)


def add_isolated_margin(api, data, *, headers={}, **kwargs):
    """ 
    Add isolated margin.

    Link: 
        https://www.kucoin.com/docs-new/rest/futures-trading/positions/add-isolated-margin
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
        data (dict): Request body parameters (JSON). See the documentation at `Link`.
        headers (dict): HTTP headers.
        kwargs:
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
    endpoint = f"/api/v1/position/margin/deposit-margin"
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

    rate_limiter.acquire('kucoin.classic_rest.futures.positions.add_isolated_margin')
    return execute_request(send, read, check, retries=1)


def remove_isolated_margin(api, data, *, headers={}, **kwargs):
    """ 
    Remove isolated margin.

    Link: 
        https://www.kucoin.com/docs-new/rest/futures-trading/positions/remove-isolated-margin
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
        data (dict): Request body parameters (JSON). See the documentation at `Link`.
        headers (dict): HTTP headers.
        kwargs:
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
    endpoint = f"/api/v1/margin/withdrawMargin"
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

    rate_limiter.acquire('kucoin.classic_rest.futures.positions.remove_isolated_margin')
    return execute_request(send, read, check, retries=1)
