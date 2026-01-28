import logging
import json
import requests
from urllib.parse import urlencode

from integrations.shared import rate_limiter
from integrations.shared.exceptions import ApiError
from integrations.shared.functions import execute_request
import integrations.shared.exchange.okx as okx

logger = logging.getLogger(__name__)

def get_balance(api, params={}, *, headers={}, **kwargs):
    """ 
    Retrieve a list of assets (with non-zero balance), remaining balance, and available amount in the trading account.

    Link: 
        https://www.okx.com/docs-v5/en/#trading-account-rest-api-get-balance
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
        params (dict):
            ccy (str): Single currency or multiple currencies (no more than 20) separated with comma, e.g. BTC or BTC,ETH.
        headers (dict): HTTP headers.
        kwargs (dict):
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
    base_url = kwargs.get('base_url', okx.BASE_URL)
    timeout = kwargs.get('timeout', okx.TIMEOUT)
    method = 'GET'
    endpoint = '/api/v5/account/balance'+ (f"?{urlencode(params)}" if params else '')
    url = base_url + endpoint

    def send(): 
        okx.sign_headers(headers, api, method, endpoint)
        return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '0': 
            raise ApiError(f"OKX returned code {code}: {body.get('msg')}", response=response, body=body)

    try:
        rate_limiter.acquire('okx.api.trading_account.rest.get_balance') 
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to get balance from OKX: %s', e); raise

def get_positions(api, params={}, *, headers={}, **kwargs):
    """ 
    Retrieve information on your positions. When the account is in net mode, net positions will be displayed, 
    and when the account is in long/short mode, long or short positions will be displayed. 
    Return in reverse chronological order using ctime.

    Link: 
        https://www.okx.com/docs-v5/en/#trading-account-rest-api-get-positions
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
        params (dict):
            instType (str): Instrument type: MARGIN, SWAP, FUTURES, OPTION
            instId (str): Instrument ID, e.g. BTC-USDT-SWAP. 
            posId (str): Single position ID or multiple position IDs (no more than 20) separated with comma.
        headers (dict): HTTP headers.
        kwargs (dict):
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
    base_url = kwargs.get('base_url', okx.BASE_URL)
    timeout = kwargs.get('timeout', okx.TIMEOUT)
    method = 'GET'
    endpoint = '/api/v5/account/positions'+ (f"?{urlencode(params)}" if params else '')
    url = base_url + endpoint

    def send(): 
        okx.sign_headers(headers, api, method, endpoint)
        return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '0': 
            raise ApiError(f"OKX returned code {code}: {body.get('msg')}", response=response, body=body)

    try:
        rate_limiter.acquire('okx.api.trading_account.rest.get_positions') 
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to get positions from OKX: %s', e); raise

def set_leverage(api, lever, mgn_mode, data={}, *, headers={}, **kwargs):
    """ 
    There are 10 different scenarios for leverage setting: see docs at `Link`

    Link: 
        https://www.okx.com/docs-v5/en/#trading-account-rest-api-set-leverage
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
        lever (str): Leverage.
        mgn_mode (str): Margin mode. isolated cross. Can only be cross if ccy is passed.
        data (dict):
            instId (str): Instrument ID. 
            ccy (str): Currency.
            posSide (str): Position side: long short
        headers (dict): HTTP headers.
        kwargs (dict):
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
    base_url = kwargs.get('base_url', okx.BASE_URL)
    timeout = kwargs.get('timeout', okx.TIMEOUT)
    method = 'POST'
    endpoint = '/api/v5/account/set-leverage'
    url = base_url + endpoint
    data['lever'] = str(lever)
    data['mgnMode'] = mgn_mode
    payload = json.dumps(data, separators=(',', ':'))
    headers['Content-Type'] = 'application/json'

    def send(): 
        okx.sign_headers(headers, api, method, endpoint, payload)
        return http.post(url, data=payload, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '0': 
            raise ApiError(f"OKX returned code {code}: {body.get('msg')}", response=response, body=body)

    try:
        rate_limiter.acquire('okx.api.trading_account.rest.set_leverage') 
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to set leverage on OKX: %s', e); raise

def increase_decrease_margin(api, data, *, headers={}, **kwargs):
    """ 
    Increase or decrease the margin of the isolated position. Margin reduction may result in the change of the actual leverage.

    Link: 
        https://www.okx.com/docs-v5/en/#trading-account-rest-api-increase-decrease-margin
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
        data (dict):
            instId (str): Instrument ID. 
            posSide (str): Position side: long, short, net. Default: net
            type (str): add: add margin; reduce: reduce margin
            amt (str): Amount to be increased or decreased.
            ccy (str): Currency. Applicable to isolated MARGIN orders.
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
    base_url = kwargs.get('base_url', okx.BASE_URL)
    timeout = kwargs.get('timeout', okx.TIMEOUT)
    method = 'POST'
    endpoint = '/api/v5/account/position/margin-balance'
    url = base_url + endpoint
    payload = json.dumps(data, separators=(',', ':'))
    headers['Content-Type'] = 'application/json'

    def send(): 
        okx.sign_headers(headers, api, method, endpoint, payload)
        return http.post(url, data=payload, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '0': 
            raise ApiError(f"OKX returned code {code}: {body.get('msg')}", response=response, body=body)

    try:
        rate_limiter.acquire('okx.api.trading_account.rest.increase_decrease_margin') 
        return execute_request(send, read, check, retries=1)
    except Exception as e: logger.error('Failed to increase/decrease margin on OKX: %s', e); raise
