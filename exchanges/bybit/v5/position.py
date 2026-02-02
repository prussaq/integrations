import logging
import requests
from urllib.parse import urlencode

from integrations.shared import rate_limiter
from integrations.shared.exceptions import ApiError
from integrations.shared.functions import execute_request
import integrations.shared.exchange.bybit as bybit

logger = logging.getLogger(__name__)

def get_position_info(api, category, params={}, *, headers={}, **kwargs):
    """ 
    Query real-time position data, such as position size, cumulative realized PNL, etc.

    Link: 
        https://bybit-exchange.github.io/docs/v5/position
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
        category (str): Product type: spot, linear, inverse, option
            If linear, either `symbol` or `settleCoin` is required. `symbol` has a higher priority.
        params (dict):
            symbol (str): Symbol name, like BTCUSDT, uppercase only.
            baseCoin (str): Base coin, uppercase only. option only. Return all option positions if not passed.
            settleCoin (str): Settle coin. 
            limit (int): Limit for data size per page. [1, 200]. Default: 20
            cursor (str): Cursor. Use the nextPageCursor token from the response to retrieve the next page of the result set.
        headers (dict): 
            X-BAPI-RECV-WINDOW (str): Exchange receive window (ms), should be tuned with `timeout` in mind. 
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
    base_url = kwargs.get('base_url', bybit.BASE_URL)
    recv_window = headers.get('X-BAPI-RECV-WINDOW', bybit.RECV_WINDOW)
    timeout = kwargs.get('timeout', bybit.TIMEOUT)
    params['category'] = category
    query = urlencode(params)
    url = f"{base_url}/v5/position/list?{query}"

    def send(): 
        bybit.sign_headers(headers, api, recv_window, query)
        return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('retCode')
        if code != 0: 
            raise ApiError(f"Bybit returned code {code}: {body.get('retMsg')}", response=response, body=body)

    try:
        rate_limiter.acquire('bybit.v5.position.get_position_info')  
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to get position info from Bybit: %s', e); raise

def set_leverage(api, category, symbol, *, buy, sell, headers={}, **kwargs):
    """
    Set leverage.
    Buy and sell leverage must be identical in both one-way mode and hedge mode when using cross margin.

    Link: 
        https://bybit-exchange.github.io/docs/v5/position/leverage
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
        category (str): Product type: linear, inverse
        symbol (str): Symbol name, like BTCUSDT, uppercase only.
        buy (str): Buy leverage.
        sell (str): Sell leverage.
        headers (dict): 
            X-BAPI-RECV-WINDOW (str): Exchange receive window (ms), should be tuned with `timeout` in mind. 
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
    base_url = kwargs.get('base_url', bybit.BASE_URL)
    recv_window = headers.get('X-BAPI-RECV-WINDOW', bybit.RECV_WINDOW)
    timeout = kwargs.get('timeout', bybit.TIMEOUT)
    url = f"{base_url}/v5/position/set-leverage"
    payload = f'{{"category":"{category}","symbol":"{symbol}","buyLeverage":"{buy}","sellLeverage":"{sell}"}}'
    headers['Content-Type'] = 'application/json'

    def send(): 
        bybit.sign_headers(headers, api, recv_window, payload)
        return http.post(url, data=payload, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('retCode')
        if code != 0: 
            raise ApiError(f"Bybit returned code {code}: {body.get('retMsg')}", response=response, body=body)

    try:
        rate_limiter.acquire('bybit.v5.position.set_leverage')  
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to set leverage on Bybit: %s', e); raise

def set_trading_stop(api, data, *, headers={}, **kwargs):
    """
    Set the take profit, stop loss or trailing stop for the position.

    Link: 
        https://bybit-exchange.github.io/docs/v5/position/trading-stop
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
        data (dict): Request body parameters (JSON). See the documentation at `Link`.
        headers (dict): 
            X-BAPI-RECV-WINDOW (str): Exchange receive window (ms), should be tuned with `timeout` in mind. 
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
    base_url = kwargs.get('base_url', bybit.BASE_URL)
    recv_window = headers.get('X-BAPI-RECV-WINDOW', bybit.RECV_WINDOW)
    timeout = kwargs.get('timeout', bybit.TIMEOUT)
    url = f"{base_url}/v5/position/trading-stop"
    payload = json.dumps(data, separators=(',', ':'))
    headers['Content-Type'] = 'application/json'

    def send(): 
        bybit.sign_headers(headers, api, recv_window, payload)
        return http.post(url, data=payload, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('retCode')
        if code != 0: 
            raise ApiError(f"Bybit returned code {code}: {body.get('retMsg')}", response=response, body=body)

    try:
        rate_limiter.acquire('bybit.v5.position.set_trading_stop')  
        return execute_request(send, read, check, retries=1)
    except Exception as e: logger.error('Failed to set trading stop on Bybit: %s', e); raise

def get_closed_PnL(api, category, params={}, *, headers={}, **kwargs):
    """ 
    Query user's closed profit and loss records.

    startTime and endTime are not passed, return 7 days by default
    Only startTime is passed, return range between startTime and startTime+7 days
    Only endTime is passed, return range between endTime-7 days and endTime
    If both are passed, the rule is endTime - startTime <= 7 days

    Link: 
        https://bybit-exchange.github.io/docs/v5/position/close-pnl
    Args:
        api (dict): API credentials. See `sign_headers` api parameter.
        category (str): Product type linear(USDT Contract, USDC Contract).
        params (dict):
            symbol (str): Symbol name, like BTCUSDT, uppercase only.
            startTime (int): The start timestamp (ms).
            endTime (int): The end timestamp (ms).
            limit (int): Limit for data size per page. [1, 200]. Default: 50
            cursor (str): Cursor. Use the nextPageCursor token from the response to retrieve the next page of the result set.
        headers (dict): 
            X-BAPI-RECV-WINDOW (str): Exchange receive window (ms), should be tuned with `timeout` in mind. 
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
    base_url = kwargs.get('base_url', bybit.BASE_URL)
    recv_window = headers.get('X-BAPI-RECV-WINDOW', bybit.RECV_WINDOW)
    timeout = kwargs.get('timeout', bybit.TIMEOUT)
    params['category'] = category
    query = urlencode(params)
    url = f"{base_url}/v5/position/closed-pnl?{query}"

    def send(): 
        bybit.sign_headers(headers, api, recv_window, query)
        return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('retCode')
        if code != 0: 
            raise ApiError(f"Bybit returned code {code}: {body.get('retMsg')}", response=response, body=body)

    try:
        rate_limiter.acquire('bybit.v5.position.get_closed_PnL')  
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to get closed PnL from Bybit: %s', e); raise
