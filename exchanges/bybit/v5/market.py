import logging
import requests
from urllib.parse import urlencode

from integrations.shared import rate_limiter
from integrations.shared.exceptions import ApiError
from integrations.shared.functions import execute_request
import integrations.shared.exchange.bybit as bybit

logger = logging.getLogger(__name__)

def get_kline(symbol, interval, params={}, *, headers={}, **kwargs):
    """ 
    Query for historical klines (also known as candles/candlesticks). 
    Charts are returned in groups based on the requested interval.

    Link: 
        https://bybit-exchange.github.io/docs/v5/market/kline
    Args:
        symbol (str): Symbol name, like BTCUSDT, uppercase only.
        interval (str): Kline interval: 1,3,5,15,30,60,120,240,360,720,D,W,M
        params (dict): 
            category (str): Product type: spot,linear,inverse. Default: linear
            start (int): The start timestamp (ms).
            end (int): The end timestamp (ms).
            limit (int): Limit for data size per page. [1, 1000]. Default: 200
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
    base_url = kwargs.get('base_url', bybit.BASE_URL)
    timeout = kwargs.get('timeout', bybit.TIMEOUT)
    url = f"{base_url}/v5/market/kline"
    params['symbol'] = symbol
    params['interval'] = interval

    def send(): return http.get(url, params=params, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('retCode')
        if code != 0: 
            raise ApiError(f"Bybit returned code {code}: {body.get('retMsg')}", response=response, body=body)

    try:
        rate_limiter.acquire('bybit.v5.market.get_kline')  
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to get kline from Bybit: %s', e); raise

def get_instruments_info(category, params={}, *, headers={}, **kwargs):
    """ 
    Query for the instrument specification of online trading pairs. 

    Link: 
        https://bybit-exchange.github.io/docs/v5/market/instrument
    Args:
        category (str): Product type: spot, linear, inverse, option
        params (dict):
            symbol (str): Symbol name, like BTCUSDT, uppercase only.
            symbolType (str): The region to which the trading pair belongs, only for linear,inverse,spot
            status (str): Symbol status filter.
            baseCoin (str): Base coin, uppercase only.
            limit (int): Limit for data size per page. [1, 1000]. Default: 500
            cursor (str): Cursor. Use the nextPageCursor token from the response to retrieve the next page of the result set.
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
    base_url = kwargs.get('base_url', bybit.BASE_URL)
    timeout = kwargs.get('timeout', bybit.TIMEOUT)
    url = f"{base_url}/v5/market/instruments-info"
    params['category'] = category

    def send(): return http.get(url, params=params, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('retCode')
        if code != 0: 
            raise ApiError(f"Bybit returned code {code}: {body.get('retMsg')}", response=response, body=body)

    try:
        rate_limiter.acquire('bybit.v5.market.get_instruments_info')  
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to get instruments info from Bybit: %s', e); raise

def get_tickers(category, params={}, *, headers={}, **kwargs):
    """ 
    Query for the latest price snapshot, best bid/ask price, and trading volume in the last 24 hours.

    Link: 
        https://bybit-exchange.github.io/docs/v5/market/tickers
    Args:
        category (str): Product type: spot, linear, inverse, option
        params (dict):
            symbol (str): Symbol name, like BTCUSDT, uppercase only.
            baseCoin (str): Base coin, uppercase only. Apply to option only.
            expDate (str): Expiry date. e.g., 25DEC22. Apply to option only.
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
    base_url = kwargs.get('base_url', bybit.BASE_URL)
    timeout = kwargs.get('timeout', bybit.TIMEOUT)
    url = f"{base_url}/v5/market/tickers"
    params['category'] = category

    def send(): return http.get(url, params=params, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('retCode')
        if code != 0: 
            raise ApiError(f"Bybit returned code {code}: {body.get('retMsg')}", response=response, body=body)

    try:
        rate_limiter.acquire('bybit.v5.market.get_tickers')  
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to get tickers from Bybit: %s', e); raise

def get_funding_rate_history(category, symbol, params={}, *, headers={}, **kwargs):
    """ 
    Query for historical funding rates.

    Link: 
        https://bybit-exchange.github.io/docs/v5/market/history-fund-rate
    Args:
        category (str): Product type: spot, linear, inverse, option
        symbol (str): Symbol name, like BTCUSDT, uppercase only.
        params (dict):
            startTime (int): The start timestamp (ms).
            endTime (int): The end timestamp (ms).
            limit (int): Limit for data size per page. [1, 200]. Default: 200
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
    base_url = kwargs.get('base_url', bybit.BASE_URL)
    timeout = kwargs.get('timeout', bybit.TIMEOUT)
    url = f"{base_url}/v5/market/funding/history"
    params['category'] = category
    params['symbol'] = symbol

    def send(): return http.get(url, params=params, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('retCode')
        if code != 0: 
            raise ApiError(f"Bybit returned code {code}: {body.get('retMsg')}", response=response, body=body)

    try:
        rate_limiter.acquire('bybit.v5.market.get_funding_rate_history')  
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to get funding rate history from Bybit: %s', e); raise
