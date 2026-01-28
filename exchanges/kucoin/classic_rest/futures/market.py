import logging
import requests
from urllib.parse import urlencode

from integrations.shared import rate_limiter
from integrations.shared.exceptions import ApiError
from integrations.shared.functions import execute_request
import integrations.shared.exchange.kucoin as kucoin

logger = logging.getLogger(__name__)

def get_symbol(symbol, *, headers={}, **kwargs):
    """ 
    Get information about tradable contract.

    Link: 
        https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-symbol
    Args:
        symbol (str): Symbol of the contract.
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
    base_url = kwargs.get('base_url', kucoin.FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', kucoin.TIMEOUT)
    url = f"{base_url}/api/v1/contracts/{symbol}"

    def send(): return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '200000': 
            raise ApiError(f"KuCoin returned code {code}: {body.get('msg')}", response=response, body=body)

    try:
        rate_limiter.acquire('kucoin.classic_rest.futures.market.get_symbol') 
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to get symbol from KuCoin: %s', e); raise

def get_all_symbols(*, headers={}, **kwargs):
    """ 
    Get detailed information about all tradable contracts.

    Link: 
        https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-all-symbols
    Args:
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
    base_url = kwargs.get('base_url', kucoin.FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', kucoin.TIMEOUT)
    url = f"{base_url}/api/v1/contracts/active"

    def send(): return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '200000': 
            raise ApiError(f"KuCoin returned code {code}: {body.get('msg')}", response=response, body=body)

    try:
        rate_limiter.acquire('kucoin.classic_rest.futures.market.get_all_symbols') 
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to get all symbols from KuCoin: %s', e); raise

def get_ticker(symbol, *, headers={}, **kwargs):
    """ 
    Get ticker including "last traded price/size", "best bid/ask price/size" etc. of a single symbol.

    Link: 
        https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-ticker
    Args:
        symbol (str): Symbol of the contract.
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
    base_url = kwargs.get('base_url', kucoin.FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', kucoin.TIMEOUT)
    url = f"{base_url}/api/v1/ticker?symbol={symbol}"

    def send(): return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '200000': 
            raise ApiError(f"KuCoin returned code {code}: {body.get('msg')}", response=response, body=body)

    try:
        rate_limiter.acquire('kucoin.classic_rest.futures.market.get_ticker')  
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to get ticker from KuCoin: %s', e); raise

def get_klines(symbol, granularity, params={}, *, headers={}, **kwargs):
    """ 
    Get the symbol’s candlestick chart data. Max 500 pieces per page. 
    May be incomplete if there are no ticks within interval.

    Link: 
        https://www.kucoin.com/docs-new/rest/futures-trading/market-data/get-klines
    Args:
        symbol (str): Symbol of the contract.
        granularity: Candlestick type (minutes): 1, 5, 15, 30, 60, 120, 240, 480, 720, 1440, 10080
        params (dict):
            from: Start time (milliseconds)
            to: End time (milliseconds)
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
    base_url = kwargs.get('base_url', kucoin.FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', kucoin.TIMEOUT)
    params['symbol'] = symbol
    params['granularity'] = granularity
    url = f"{base_url}/api/v1/kline/query?{urlencode(params)}"

    def send(): return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '200000': 
            raise ApiError(f"KuCoin returned code {code}: {body.get('msg')}", response=response, body=body)

    try:
        rate_limiter.acquire('kucoin.classic_rest.futures.market.get_klines')
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to get klines from KuCoin: %s', e); raise

