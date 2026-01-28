import logging
import requests
from urllib.parse import urlencode

from integrations.shared import rate_limiter
from integrations.shared.exceptions import ApiError
from integrations.shared.functions import execute_request
import integrations.shared.exchange.binance as binance

logger = logging.getLogger(__name__)

def get_kline(symbol, interval, params={}, *, headers={}, **kwargs):
    """ 
    Kline/candlestick bars for a symbol. Klines are uniquely identified by their open time.

    If startTime and endTime are not sent, the most recent klines are returned.

    Link: 
        https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Kline-Candlestick-Data
    Args:
        symbol (str): Symbol name.
        interval (str): Interval: 1m,3m,5m,15m,30m,1h,2h,4h,6h,8h,12h,1d,3d,1w,1M
        params (dict): 
            category: Product type: spot,linear,inverse. Default: linear
            startTime (long): The start timestamp (ms).
            endTime (long): The end timestamp (ms).
            limit (int): Default 500; max 1500.
        headers (dict): HTTP headers.
        kwargs (dict): 
            session (requests.Session): Must be managed by caller.
            base_url (str): Base HTTP endpoint for the exchange API.
            timeout (float | (float, float)): HTTP timeout forwarded to `requests` (connect/read).
            retries (int): Number of retry attempts.
            delay (float): Initial retry delay in seconds.
            backoff (float): Retry backoff multipheaders=headers,lier.
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
    base_url = kwargs.get('base_url', binance.USDS_FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', binance.TIMEOUT)
    params['symbol'] = symbol
    params['interval'] = interval
    url = f"{base_url}/fapi/v1/klines"

    def send(): return http.get(url, params=params, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, (dict, list)): raise ApiError("unexpected response type", response=response, body=body)
        if isinstance(body, dict) and 'code' in body:
            raise ApiError(f"Binance returned code {body['code']}: {body.get('msg')}", response=response, body=body)

    try:
        rate_limiter.acquire('binance.derivatives.usdsm_futures.market_data.rest.get_kline')  
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to get kline from Binance: %s', e); raise

def get_funding_rate_history(params={}, *, headers={}, **kwargs):
    """ 
    Get funding rate history.

    If startTime and endTime are not sent, the most recent 200 records are returned.
    If the number of data between startTime and endTime is larger than limit, return as startTime + limit.
    In ascending order.

    Link: 
        https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-History
    Args:
        params (dict): 
            symbol (str): Symbol name.
            startTime (long): Timestamp in ms to get funding rate from INCLUSIVE.
            endTime (long): Timestamp in ms to get funding rate until INCLUSIVE.
            limit (int): Default 100; max 1000
        headers (dict): HTTP headers.
        kwargs (dict): 
            session (requests.Session): Must be managed by caller.
            base_url (str): Base HTTP endpoint for the exchange API.
            timeout (float | (float, float)): HTTP timeout forwarded to `requests` (connect/read).
            retries (int): Number of retry attempts.
            delay (float): Initial retry delay in seconds.
            backoff (float): Retry backoff multipheaders=headers,lier.
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
    base_url = kwargs.get('base_url', binance.USDS_FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', binance.TIMEOUT)
    url = f"{base_url}/fapi/v1/fundingRate"

    def send(): return http.get(url, params=params, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, (dict, list)): raise ApiError("unexpected response type", response=response, body=body)
        if isinstance(body, dict) and 'code' in body:
            raise ApiError(f"Binance returned code {body['code']}: {body.get('msg')}", response=response, body=body)

    try:
        rate_limiter.acquire('binance.derivatives.usdsm_futures.market_data.rest.get_funding_rate_history')  
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to get funding rate history from Binance: %s', e); raise

def get_funding_rate_info(*, headers={}, **kwargs):
    """ 
    Query funding rate info for symbols that had FundingRateCap/ FundingRateFloor / fundingIntervalHours adjustment.

    Link: 
        https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Get-Funding-Rate-Info
    Args:
        headers (dict): HTTP headers.
        kwargs (dict): 
            session (requests.Session): Must be managed by caller.
            base_url (str): Base HTTP endpoint for the exchange API.
            timeout (float | (float, float)): HTTP timeout forwarded to `requests` (connect/read).
            retries (int): Number of retry attempts.
            delay (float): Initial retry delay in seconds.
            backoff (float): Retry backoff multipheaders=headers,lier.
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
    base_url = kwargs.get('base_url', binance.USDS_FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', binance.TIMEOUT)
    url = f"{base_url}/fapi/v1/fundingInfo"

    def send(): return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, (dict, list)): raise ApiError("unexpected response type", response=response, body=body)
        if isinstance(body, dict) and 'code' in body:
            raise ApiError(f"Binance returned code {body['code']}: {body.get('msg')}", response=response, body=body)

    try:
        rate_limiter.acquire('binance.derivatives.usdsm_futures.market_data.rest.get_funding_rate_info')  
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to get funding rate info from Binance: %s', e); raise

def get_price_ticker_v2(params={}, *, headers={}, **kwargs):
    """ 
    Get latest price for a symbol or symbols.

    If the symbol is not sent, prices for all symbols will be returned in an array.

    Link: 
        https://developers.binance.com/docs/derivatives/usds-margined-futures/market-data/rest-api/Symbol-Price-Ticker-v2
    Args:
        params (dict): 
            symbol (str): Symbol name.
        headers (dict): HTTP headers.
        kwargs (dict): 
            session (requests.Session): Must be managed by caller.
            base_url (str): Base HTTP endpoint for the exchange API.
            timeout (float | (float, float)): HTTP timeout forwarded to `requests` (connect/read).
            retries (int): Number of retry attempts.
            delay (float): Initial retry delay in seconds.
            backoff (float): Retry backoff multipheaders=headers,lier.
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
    base_url = kwargs.get('base_url', binance.USDS_FUTURES_BASE_URL)
    timeout = kwargs.get('timeout', binance.TIMEOUT)
    url = f"{base_url}/fapi/v2/ticker/price"

    def send(): return http.get(url, params=params, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, (dict, list)): raise ApiError("unexpected response type", response=response, body=body)
        if isinstance(body, dict) and 'code' in body:
            raise ApiError(f"Binance returned code {body['code']}: {body.get('msg')}", response=response, body=body)

    try:
        rate_limiter.acquire('binance.derivatives.usdsm_futures.market_data.rest.get_price_ticker_v2')  
        return execute_request(send, read, check, kwargs)
    except Exception as e: logger.error('Failed to get price ticker (V2) from Binance: %s', e); raise
