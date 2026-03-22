import logging
import requests

from integrations.shared import rate_limiter
from integrations.shared.exceptions import ApiError
from integrations.shared.functions import execute_request
import integrations.shared.exchange.kucoin as kucoin

logger = logging.getLogger(__name__)


def get_symbol(symbol, *, headers={}, **kwargs):
    """ 
    Request via this endpoint to get detail currency pairs for trading.

    Link: 
        https://www.kucoin.com/docs-new/rest/spot-trading/market-data/get-symbol
    Args:
        symbol (str): Symbol. Example: BTC-USDT
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
    base_url = kwargs.get('base_url', kucoin.SPOT_BASE_URL)
    timeout = kwargs.get('timeout', kucoin.TIMEOUT)
    url = f"{base_url}/api/v2/symbols/{symbol}"

    def send(): return http.get(url, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '200000': 
            raise ApiError(f"KuCoin returned code {code}: {body.get('msg')}", response=response, body=body)

    rate_limiter.acquire('kucoin.classic_rest.spot.market.get_symbol') 
    return execute_request(send, read, check, kwargs)


def get_all_symbols(params={}, *, headers={}, **kwargs):
    """ 
    Request a list of available currency pairs for trading via this endpoint.

    Link: 
        https://www.kucoin.com/docs-new/rest/spot-trading/market-data/get-all-symbols
    Args:
        params (dict):
            market (str): The trading market. Examples: ALTS, USDS, ETF
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
    base_url = kwargs.get('base_url', kucoin.SPOT_BASE_URL)
    timeout = kwargs.get('timeout', kucoin.TIMEOUT)
    url = f"{base_url}/api/v2/symbols"

    def send(): return http.get(url, params=params, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '200000': 
            raise ApiError(f"KuCoin returned code {code}: {body.get('msg')}", response=response, body=body)

    rate_limiter.acquire('kucoin.classic_rest.spot.market.get_all_symbols') 
    return execute_request(send, read, check, kwargs)


def get_klines(symbol, type, params={}, *, headers={}, **kwargs):
    """ 
    Get the Kline of the symbol. Data are returned in grouped buckets based on requested type.
    For each query, the system would return at most 1500 pieces of data. To obtain more data, please page the data by time.
    Klines data may be incomplete. No data is published for intervals where there are no ticks.

    Link: 
        https://www.kucoin.com/docs-new/rest/spot-trading/market-data/get-klines
    Args:
        symbol (str): Symbol. Example: BTC-USDT
        type (str): Type of candlestick: 1min, 3min, 5min, 15min, 30min, 1hour, 2hour, 4hour, 6hour, 8hour, 12hour, 1day, 1week, 1month
        params (dict):
            startAt (int): Start time (second), default is 0
            endAt (int): End time (second), default is 0
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
    base_url = kwargs.get('base_url', kucoin.SPOT_BASE_URL)
    timeout = kwargs.get('timeout', kucoin.TIMEOUT)
    params['symbol'] = symbol
    params['type'] = type
    url = f"{base_url}/api/v1/market/candles"

    def send(): return http.get(url, params=params, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)
        code = body.get('code')
        if code != '200000': 
            raise ApiError(f"KuCoin returned code {code}: {body.get('msg')}", response=response, body=body)

    rate_limiter.acquire('kucoin.classic_rest.spot.market.get_klines') 
    return execute_request(send, read, check, kwargs)

