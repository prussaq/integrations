import logging
import requests

from integrations.shared import rate_limiter
from integrations.shared.exceptions import ApiError
from integrations.shared.functions import execute_request
import integrations.shared.exchange.dydx as dydx

logger = logging.getLogger(__name__)


def get_perpetual_markets(params=None, **kwargs):
    """ 
    Retrieves perpetual markets..

    Link: 
        https://docs.dydx.xyz/indexer-client/http#get-perpetual-markets
    Args:
        params (dict):
            market (str): The specific market ticker to retrieve. If not provided, all markets are returned.
            limit (int): Maximum number of asset positions to return in the response.
        kwargs: 
            session (requests.Session): Must be managed by caller.
            base_url (str): Base HTTP endpoint for the exchange API.
            retries (int): Number of retry attempts.
            delay (float): Initial retry delay in seconds.
            backoff (float): Retry backoff multiplier.
            full (bool): If True, return both the parsed response body and the HTTP response object.
            Additional `requests` params like timeout, headers, etc.
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
    http = kwargs.pop('session', requests)
    base_url = kwargs.pop('base_url', dydx.INDEXER_MAINNET_HTTP)
    timeout = kwargs.pop('timeout', dydx.TIMEOUT)
    if params is None: params = {}
    url = f"{base_url}/v4/perpetualMarkets"

    def send(settings): return http.get(url, params=params, timeout=timeout, **settings)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)

    rate_limiter.acquire('dydx.indexer.http.markets.get_perpetual_markets') 
    return execute_request(send, read, check, kwargs)


def get_candles(market, resolution, params=None, **kwargs):
    """ 
    Retrieves candle data for a specific perpetual market.

    Link: 
        https://docs.dydx.xyz/indexer-client/http#get-candles
    Args:
        market (str): The market ticker.
        resolution (str): The candle resolution: 1MIN,5MINS,15MINS,30MINS,1HOUR,4HOURS,1DAY
        params (dict):
            limit (int): The maximum number of candles to retrieve.
            fromISO (str): The start timestamp in ISO format.
            toISO (str): The end timestamp in ISO format.
        kwargs: 
            session (requests.Session): Must be managed by caller.
            base_url (str): Base HTTP endpoint for the exchange API.
            retries (int): Number of retry attempts.
            delay (float): Initial retry delay in seconds.
            backoff (float): Retry backoff multiplier.
            full (bool): If True, return both the parsed response body and the HTTP response object.
            Additional `requests` params like timeout, headers, etc.
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
    http = kwargs.pop('session', requests)
    base_url = kwargs.pop('base_url', dydx.INDEXER_MAINNET_HTTP)
    timeout = kwargs.pop('timeout', dydx.TIMEOUT)
    if params is None: params = {}
    params['resolution'] = resolution
    url = f"{base_url}/v4/candles/perpetualMarkets/{market}"

    def send(settings): return http.get(url, params=params, timeout=timeout, **settings)
    def read(response): return response.json()
    def check(response, body):
        if not isinstance(body, dict): raise ApiError("unexpected response type", response=response, body=body)

    rate_limiter.acquire('dydx.indexer.http.markets.get_candles') 
    return execute_request(send, read, check, kwargs)
