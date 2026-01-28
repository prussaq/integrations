import logging
import requests
from urllib.parse import urlencode

from integrations.shared import rate_limiter
from integrations.shared.exceptions import ApiError
from integrations.shared.functions import execute_request
import integrations.shared.exchange.bitget as bitget

logger = logging.getLogger(__name__)

def get_ticker(symbol, product_type, *, headers={}, **kwargs):
    """ 
    Get ticker data of the given 'productType' and 'symbol'.

    Link: 
        https://www.bitget.com/api-doc/contract/market/Get-Ticker
    Args:
        symbol (str): Trading pair.
        product_type (str): Product type: USDT-FUTURES, COIN-FUTURES, USDC-FUTURES
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
    base_url = kwargs.get('base_url', bitget.MAIN_DOMAIN)
    timeout = kwargs.get('timeout', bitget.TIMEOUT)
    url = f"{base_url}/api/v2/mix/market/ticker?productType={product_type}&symbol={symbol}"

    def send(): return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '00000': 
            raise ApiError(f"Bitget returned code {code}: {body.get('msg')}", response=response, body=body)

    try:
        rate_limiter.acquire('bitget.futures.market.get_ticker')  
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to get ticker from Bitget: %s', e); raise

def get_candlestick_data(symbol, product_type, granularity, params={}, *, headers={}, **kwargs):
    """ 
    By default, 100 records are returned. If there is no data, an empty array is returned. 
    The queryable data history varies depending on the k-line granularity.

    Link: 
        https://www.bitget.com/api-doc/contract/market/Get-Candle-Data
    Args:
        symbol (str): Trading pair.
        product_type (str): Product type: USDT-FUTURES, COIN-FUTURES, USDC-FUTURES
        granularity (str): K-line size: 1m,3m,5m,15m,30m,1H,4H,6H,12H,1D,3D,1W,1M,6Hutc,12Hutc,1Dutc,3Dutc,1Wutc,1Mutc
        params (dict):
            startTime (int): The start time is to query the k-lines after this time (ms).
            endTime (int): The end time is to query the k-lines before this time (ms).
            kLineType (str): Candlestick chart types: MARKET tick; MARK mark; INDEX index; MARKET by default.
            limit (int): Default: 100, maximum: 1000
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
    base_url = kwargs.get('base_url', bitget.MAIN_DOMAIN)
    timeout = kwargs.get('timeout', bitget.TIMEOUT)
    url = f"{base_url}/api/v2/mix/market/candles"
    params['symbol'] = symbol
    params['productType'] = product_type
    params['granularity'] = granularity

    def send(): return http.get(url, params=params, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '00000': 
            raise ApiError(f"Bitget returned code {code}: {body.get('msg')}", response=response, body=body)

    try:
        rate_limiter.acquire('bitget.futures.market.get_candlestick_data')  
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to get candlestick data from Bitget: %s', e); raise

def get_next_funding_time(symbol, product_type, *, headers={}, **kwargs):
    """ 
    Get the next settlement time of the contract and the settlement period of the contract

    Link: 
        https://www.bitget.com/api-doc/contract/market/Get-Symbol-Next-Funding-Time
    Args:
        symbol (str): Trading pair.
        product_type (str): Product type: USDT-FUTURES, COIN-FUTURES, USDC-FUTURES
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
    base_url = kwargs.get('base_url', bitget.MAIN_DOMAIN)
    timeout = kwargs.get('timeout', bitget.TIMEOUT)
    url = f"{base_url}/api/v2/mix/market/funding-time?productType={product_type}&symbol={symbol}"

    def send(): return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '00000': 
            raise ApiError(f"Bitget returned code {code}: {body.get('msg')}", response=response, body=body)

    try:
        rate_limiter.acquire('bitget.futures.market.get_next_funding_time')  
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to get next funding time from Bitget: %s', e); raise

def get_historical_funding_rates(symbol, product_type, params={}, *, headers={}, **kwargs):
    """ 
    Get the historical funding rate of the contract

    Link: 
        https://www.bitget.com/api-doc/contract/market/Get-History-Funding-Rate
    Args:
        symbol (str): Trading pair.
        product_type (str): Product type: USDT-FUTURES, COIN-FUTURES, USDC-FUTURES
        params (dict):
            pageSize (int): Number of queries: Default: 20, maximum: 100.
            pageNo (int): Page number.
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
    base_url = kwargs.get('base_url', bitget.MAIN_DOMAIN)
    timeout = kwargs.get('timeout', bitget.TIMEOUT)
    url = f"{base_url}/api/v2/mix/market/history-fund-rate"
    params['symbol'] = symbol
    params['productType'] = product_type

    def send(): return http.get(url, params=params, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '00000': 
            raise ApiError(f"Bitget returned code {code}: {body.get('msg')}", response=response, body=body)

    try:
        rate_limiter.acquire('bitget.futures.market.get_historical_funding_rates')  
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to get historical funding rates from Bitget: %s', e); raise

def get_current_funding_rate(product_type, params={}, *, headers={}, **kwargs):
    """ 
    Get the current funding rate of the contract

    Link: 
        https://www.bitget.com/api-doc/contract/market/Get-Current-Funding-Rate
    Args:
        product_type (str): Product type: USDT-FUTURES, COIN-FUTURES, USDC-FUTURES
        params (dict):
            symbol (str): Trading pair.
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
    base_url = kwargs.get('base_url', bitget.MAIN_DOMAIN)
    timeout = kwargs.get('timeout', bitget.TIMEOUT)
    url = f"{base_url}/api/v2/mix/market/current-fund-rate"
    params['productType'] = product_type

    def send(): return http.get(url, params=params, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '00000': 
            raise ApiError(f"Bitget returned code {code}: {body.get('msg')}", response=response, body=body)

    try:
        rate_limiter.acquire('bitget.futures.market.get_current_funding_rate')  
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to get current funding rate from Bitget: %s', e); raise
