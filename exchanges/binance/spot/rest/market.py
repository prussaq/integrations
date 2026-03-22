import logging
import requests

from integrations.shared import rate_limiter
from integrations.shared.exceptions import ApiError
from integrations.shared.functions import execute_request
import integrations.shared.exchange.binance as binance

logger = logging.getLogger(__name__)


def get_kline(symbol, interval, params={}, *, headers={}, **kwargs):
    """ 
    Kline/candlestick bars for a symbol. Klines are uniquely identified by their open time.
    
    Link: 
        https://developers.binance.com/docs/binance-spot-api-docs/rest-api/market-data-endpoints#klinecandlestick-data
    Args:
        symbol (str): Symbol name.
        interval (str): Interval: 1s,1m,3m,5m,15m,30m,1h,2h,4h,6h,8h,12h,1d,3d,1w,1M
        params (dict): 
            startTime (long): The start timestamp (ms).
            endTime (long): The end timestamp (ms).
            timeZone (str): Default: 0 (UTC)
            limit (int): Default: 500; Maximum: 1000.
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
    base_url = kwargs.get('base_url', binance.SPOT_PUBLIC_MARKET_DATA_BASE_URL)
    timeout = kwargs.get('timeout', binance.TIMEOUT)
    params['symbol'] = symbol
    params['interval'] = interval
    url = f"{base_url}/api/v3/klines"

    def send(): return http.get(url, params=params, headers=headers, timeout=timeout)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, (dict, list)): raise ApiError("unexpected response type", response=response, body=body)
        if isinstance(body, dict) and 'code' in body:
            raise ApiError(f"Binance returned code {body['code']}: {body.get('msg')}", response=response, body=body)

    rate_limiter.acquire('binance.spot.rest.market.get_kline')  
    return execute_request(send, read, check, kwargs)

